
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

// Greedy Lion - synchronized game rounds
// Real betting, wallet debits and payouts remain disabled.
const GAME_ROOM = 'maxchat_greedy_lion_global';
const ROUND_MS = 25_000;
const FOOD_COUNT = 8;
const FOOD_MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];

let roundNumber = 1;
let roundStartsAt = Date.now();
let roundEndsAt = roundStartsAt + ROUND_MS;
let history = [];

function gameState() {
    return {
        roundNumber,
        roundStartsAt,
        roundEndsAt,
        serverTime: Date.now(),
        phase: 'betting_disabled',
        history: history.slice(),
        bettingEnabled: false
    };
}

function closeDueRound() {
    const now = Date.now();

    if (now < roundEndsAt) {
        return;
    }

    const finishedRound = roundNumber;
    const winningIndex = randomInt(FOOD_COUNT);

    const result = {
        roundNumber: finishedRound,
        winningIndex,
        completedAt: now,
        hasSettledBets: false
    };

    history.push(result);

    if (history.length > 8) {
        history.shift();
    }

    io.to(GAME_ROOM).emit(
        'greedy_lion_round_result',
        result
    );

    roundNumber += 1;
    roundStartsAt = now;
    roundEndsAt = now + ROUND_MS;

    io.to(GAME_ROOM).emit(
        'greedy_lion_state',
        gameState()
    );
}

// Broadcast the authoritative clock to all game clients.
setInterval(() => {
    closeDueRound();

    io.to(GAME_ROOM).emit('greedy_lion_clock', {
        roundNumber,
        roundEndsAt,
        serverTime: Date.now()
    });
}, 1000);

io.on('connection', (socket) => {
    console.log('مستخدم متصل بالسيرفر');

    // Existing voice-room events
    socket.on('join_room', (data) => {
        const { roomId, userId, userName } = data;

        socket.join(roomId);

        socket.to(roomId).emit('user_joined_room', {
            userId,
            userName
        });
    });

    socket.on('toggle_mute', (data) => {
        const { roomId, userId, isMuted } = data;

        io.to(roomId).emit('user_toggle_mute', {
            userId,
            isMuted
        });
    });

    socket.on('sit_on_seat', (data) => {
        const { roomId, userId, seatIndex } = data;

        io.to(roomId).emit('user_sat_on_seat', {
            userId,
            seatIndex
        });
    });

    socket.on('leave_seat', (data) => {
        const { roomId, userId } = data;

        io.to(roomId).emit('user_left_seat', {
            userId
        });
    });

    socket.on('start_audio_stream', (data) => {
        const { roomId, userId } = data;

        socket.to(roomId).emit('user_started_audio', {
            userId
        });
    });

    socket.on('stop_audio_stream', (data) => {
        const { roomId, userId } = data;

        socket.to(roomId).emit('user_stopped_audio', {
            userId
        });
    });

    socket.on('voice_data', (data) => {
        const { roomId, audioBuffer, userId } = data;

        socket.to(roomId).emit('voice_data', {
            audioBuffer,
            userId
        });
    });

    // Greedy Lion events
    socket.on('greedy_lion_join', () => {
        closeDueRound();

        socket.join(GAME_ROOM);

        socket.emit(
            'greedy_lion_state',
            gameState()
        );
    });

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
