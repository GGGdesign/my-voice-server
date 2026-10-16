
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { randomInt } = require('crypto');
const fs = require('fs');
const admin = require('firebase-admin');

// Firebase Admin - Render Secret Files
// Never upload the Firebase private key to GitHub.
const FIREBASE_KEY_PATH = '/etc/secrets/firebase-service-account.json';

try {
    if (fs.existsSync(FIREBASE_KEY_PATH)) {
        const credentials = JSON.parse(
            fs.readFileSync(FIREBASE_KEY_PATH, 'utf8')
        );

        admin.initializeApp({
            credential: admin.credential.cert(credentials),
            databaseURL: 'https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app'
        });

        console.log('Firebase Admin initialized successfully.');
    } else {
        console.warn('Firebase secret file not found. Wallet disabled.');
    }
} catch (error) {
    console.error('Firebase initialization failed:', error.message);
}

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    }
});

// Greedy Lion - persistent rounds and authenticated wallet betting.
// Keep betting disabled until security and settlement tests are complete.
const GAME_ROOM = 'maxchat_greedy_lion_global';
const ROUND_MS = 25_000;
const FOOD_COUNT = 8;
const FOOD_MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];
const CHIPS = new Set([100, 500, 1000, 5000, 10000]);

// Safety lock: do not enable real wallet betting yet.
const BETTING_ENABLED = false;

const GAME_PATH = 'greedyLionGame';
let game = null;
let busy = false;

const db = () => admin.database();

function publicState() {
    const now = Date.now();

    return {
        roundNumber: game?.roundNumber || 1,
        roundStartsAt: game?.roundStartsAt || now,
        roundEndsAt: game?.roundEndsAt || now + ROUND_MS,
        serverTime: now,
        phase: BETTING_ENABLED && admin.apps.length
            ? 'betting'
            : 'betting_disabled',
        history: (game?.history || []).slice(-8),
        bettingEnabled: Boolean(
            BETTING_ENABLED && admin.apps.length
        )
    };
}

async function loadGame() {
    if (!admin.apps.length) {
        const now = Date.now();

        game = {
            roundNumber: 1,
            roundStartsAt: now,
            roundEndsAt: now + ROUND_MS,
            history: []
        };

        return;
    }

    const ref = db().ref(GAME_PATH);
    const now = Date.now();

    await ref.transaction(old => old || {
        roundNumber: 1,
        roundStartsAt: now,
        roundEndsAt: now + ROUND_MS,
        history: []
    });

    game = (await ref.once('value')).val();
}

async function settleRound(round, winningIndex) {
    const users = (
        await db().ref('Users').once('value')
    ).val() || {};

    for (const uid of Object.keys(users)) {
        const roundBets =
            users[uid]?.lionBets?.[String(round)];

        if (!roundBets || typeof roundBets !== 'object') {
            continue;
        }

        const userRef = db().ref('Users/' + uid);

        const result = await userRef.transaction(user => {
            if (!user || !user.lionBets?.[String(round)]) {
                return;
            }

            const bets = user.lionBets[String(round)];

            if (bets.settled === true) {
                return;
            }

            let payout = 0;

            for (const entry of Object.values(
                bets.entries || {}
            )) {
                if (
                    entry.index === winningIndex &&
                    CHIPS.has(entry.amount)
                ) {
                    payout +=
                        entry.amount *
                        FOOD_MULTIPLIERS[winningIndex];
                }
            }

            if (
                !Number.isSafeInteger(payout) ||
                !Number.isSafeInteger(user.coins + payout)
            ) {
                return;
            }

            user.coins += payout;
            bets.settled = true;
            bets.payout = payout;
            bets.winningIndex = winningIndex;

            return user;
        }, undefined, false);

        if (
            !result.committed &&
            roundBets.settled !== true
        ) {
            console.error(
                'Settlement requires retry for uid',
                uid,
                'round',
                round
            );
        }
    }
}

async function tickGame() {
    if (busy || !game) {
        return;
    }

    busy = true;

    try {
        const now = Date.now();

        if (admin.apps.length) {
            const latest = (
                await db().ref(GAME_PATH).once('value')
            ).val();

            if (latest) {
                game = latest;
            }
        }

        if (now >= game.roundEndsAt) {
            const finished = game.roundNumber;

            const existing = (game.history || []).find(
                h => h.roundNumber === finished
            );

            const winningIndex = existing
                ? existing.winningIndex
                : randomInt(FOOD_COUNT);

            if (admin.apps.length) {
                const tx = await db()
                    .ref(GAME_PATH)
                    .transaction(old => {
                        if (
                            !old ||
                            old.roundNumber !== finished ||
                            Date.now() < old.roundEndsAt
                        ) {
                            return;
                        }

                        const result = {
                            roundNumber: finished,
                            winningIndex,
                            completedAt: Date.now(),
                            hasSettledBets: false
                        };

                        old.history = [
                            ...(old.history || []),
                            result
                        ].slice(-8);

                        old.roundNumber += 1;
                        old.roundStartsAt = Date.now();
                        old.roundEndsAt =
                            old.roundStartsAt + ROUND_MS;

                        return old;
                    }, undefined, false);

                game = tx.snapshot.val() || game;
            } else {
                game.history = [
                    ...(game.history || []),
                    {
                        roundNumber: finished,
                        winningIndex,
                        completedAt: now,
                        hasSettledBets: false
                    }
                ].slice(-8);

                game.roundNumber += 1;
                game.roundStartsAt = now;
                game.roundEndsAt = now + ROUND_MS;
            }

            const result = (game.history || []).find(
                h => h.roundNumber === finished
            );

            if (result) {
                io.to(GAME_ROOM).emit(
                    'greedy_lion_round_result',
                    result
                );
            }

            io.to(GAME_ROOM).emit(
                'greedy_lion_state',
                publicState()
            );
        }

        if (BETTING_ENABLED && admin.apps.length) {
            const users = (
                await db().ref('Users').once('value')
            ).val() || {};

            const rounds = new Set();

            for (const user of Object.values(users)) {
                for (const [round, bets] of Object.entries(
                    user.lionBets || {}
                )) {
                    if (!bets.settled) {
                        rounds.add(Number(round));
                    }
                }
            }

            for (const round of rounds) {
                const result = (game.history || []).find(
                    h => h.roundNumber === round
                );

                if (result) {
                    await settleRound(
                        round,
                        result.winningIndex
                    );
                } else {
                    console.error(
                        'Unsettled round absent from history:',
                        round
                    );
                }
            }
        }

        io.to(GAME_ROOM).emit(
            'greedy_lion_clock',
            {
                roundNumber: game.roundNumber,
                roundEndsAt: game.roundEndsAt,
                serverTime: Date.now()
            }
        );
    } catch (err) {
        console.error('Greedy Lion tick failed:', err);
    } finally {
        busy = false;
    }
}

async function placeBet(socket, data, ack) {
    const reply = typeof ack === 'function'
        ? ack
        : () => {};

    if (!BETTING_ENABLED || !admin.apps.length) {
        return reply({
            ok: false,
            error: 'BETTING_DISABLED'
        });
    }

    try {
        const {
            idToken,
            betId,
            index,
            amount,
            roundNumber
        } = data || {};

        if (
            typeof idToken !== 'string' ||
            typeof betId !== 'string' ||
            !/^[a-zA-Z0-9_-]{8,80}$/.test(betId) ||
            !Number.isInteger(index) ||
            index < 0 ||
            index >= FOOD_COUNT ||
            !CHIPS.has(amount) ||
            !Number.isInteger(roundNumber)
        ) {
            return reply({
                ok: false,
                error: 'INVALID_BET'
            });
        }

        const decoded = await admin.auth()
            .verifyIdToken(idToken, true);

        const current = (
            await db().ref(GAME_PATH).once('value')
        ).val();

        if (
            !current ||
            current.roundNumber !== roundNumber ||
            Date.now() >= current.roundEndsAt - 1000
        ) {
            return reply({
                ok: false,
                error: 'ROUND_CLOSED'
            });
        }

        let failure = 'INSUFFICIENT_COINS';

        const tx = await db()
            .ref('Users/' + decoded.uid)
            .transaction(user => {
                if (
                    !user ||
                    !Number.isSafeInteger(user.coins) ||
                    user.coins < 0
                ) {
                    return;
                }

                user.lionBets = user.lionBets || {};

                const key = String(roundNumber);

                const round = user.lionBets[key] || {
                    entries: {},
                    settled: false
                };

                if (round.entries?.[betId]) {
                    failure = 'DUPLICATE';
                    return;
                }

                if (round.settled) {
                    failure = 'ROUND_CLOSED';
                    return;
                }

                if (user.coins < amount) {
                    return;
                }

                const selectedSlots = new Set(
                    Object.values(
                        round.entries || {}
                    ).map(b => b.index)
                );

                if (
                    !selectedSlots.has(index) &&
                    selectedSlots.size >= 6
                ) {
                    failure = 'MAX_SLOTS';
                    return;
                }

                user.coins -= amount;

                round.entries = round.entries || {};

                round.entries[betId] = {
                    index,
                    amount
                };

                user.lionBets[key] = round;

                return user;
            }, undefined, false);

        if (!tx.committed) {
            return reply({
                ok: false,
                error: failure
            });
        }

        const updated = tx.snapshot.val();

        reply({
            ok: true,
            roundNumber,
            betId,
            index,
            amount,
            balance: updated.coins
        });

        socket.emit(
            'greedy_lion_bet_accepted',
            {
                roundNumber,
                betId,
                index,
                amount,
                balance: updated.coins
            }
        );
    } catch (err) {
        console.error('Bet failed:', err.message);

        reply({
            ok: false,
            error: 'SERVER_ERROR'
        });
    }
}

setInterval(tickGame, 1000);

loadGame()
    .then(tickGame)
    .catch(err => {
        console.error('Game startup failed:', err);
    });

io.on('connection', (socket) => {
    console.log('مستخدم متصل بالسيرفر');

    // Existing voice-room events
    socket.on('join_room', (data) => {
        const { roomId, userId, userName } = data;

        socket.join(roomId);

        socket.to(roomId).emit(
            'user_joined_room',
            {
                userId,
                userName
            }
        );
    });

    socket.on('toggle_mute', (data) => {
        const {
            roomId,
            userId,
            isMuted
        } = data;

        io.to(roomId).emit(
            'user_toggle_mute',
            {
                userId,
                isMuted
            }
        );
    });

    socket.on('sit_on_seat', (data) => {
        const {
            roomId,
            userId,
            seatIndex
        } = data;

        io.to(roomId).emit(
            'user_sat_on_seat',
            {
                userId,
                seatIndex
            }
        );
    });

    socket.on('leave_seat', (data) => {
        const {
            roomId,
            userId
        } = data;

        io.to(roomId).emit(
            'user_left_seat',
            {
                userId
            }
        );
    });

    socket.on('start_audio_stream', (data) => {
        const {
            roomId,
            userId
        } = data;

        socket.to(roomId).emit(
            'user_started_audio',
            {
                userId
            }
        );
    });

    socket.on('stop_audio_stream', (data) => {
        const {
            roomId,
            userId
        } = data;

        socket.to(roomId).emit(
            'user_stopped_audio',
            {
                userId
            }
        );
    });

    socket.on('voice_data', (data) => {
        const {
            roomId,
            audioBuffer,
            userId
        } = data;

        socket.to(roomId).emit(
            'voice_data',
            {
                audioBuffer,
                userId
            }
        );
    });

    // Greedy Lion events
    socket.on('greedy_lion_join', () => {
        socket.join(GAME_ROOM);

        socket.emit(
            'greedy_lion_state',
            publicState()
        );
    });

    socket.on(
        'greedy_lion_place_bet',
        (data, ack) => placeBet(socket, data, ack)
    );

    socket.on('greedy_lion_leave', () => {
        socket.leave(GAME_ROOM);
    });

    socket.on('disconnect', () => {
        console.log('مستخدم غادر السيرفر');
    });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, () => {
    console.log(
        `السيرفر الصوتي يعمل بنجاح على المنفذ ${PORT}`
    );
});
