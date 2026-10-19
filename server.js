
const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const PORT = process.env.PORT || 10000;
const DB_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";
const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GAME_PATH = "greedyLionGame";
const GAME_ROOM = "maxchat_greedy_lion_global";
const ROUND_MS = 25000;
const MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];
const CHIPS = [100, 500, 1000, 5000, 10000];

const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS === "true";

let firebaseReady = false;
let game = null;
let roundProcessing = false;
let settlementProcessing = false;
let loading = false;
let lastSync = 0;

try {
  const credentials = JSON.parse(
    fs.readFileSync(SECRET_PATH, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
    databaseURL: DB_URL
  });

  firebaseReady = true;
  console.log("FIREBASE READY");
} catch (error) {
  console.error("FIREBASE ERROR:", error.message);
}

const db = () => admin.database();
const gameRef = () => db().ref(GAME_PATH);
const userRef = uid => db().ref(`Users/${uid}`);

function validGame(g) {
  return Boolean(
    g &&
    Number.isSafeInteger(g.roundNumber) &&
    g.roundNumber > 0 &&
    Number.isFinite(Number(g.roundStartsAt)) &&
    Number.isFinite(Number(g.roundEndsAt))
  );
}

function historyArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).slice(-8);
  }

  if (value && typeof value === "object") {
    return Object.values(value)
      .filter(Boolean)
      .sort((a, b) =>
        Number(a.roundNumber || 0) -
        Number(b.roundNumber || 0)
      )
      .slice(-8);
  }

  return [];
}

function createGame(now) {
  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    activeBets: {},
    pendingSettlement: {},
    history: []
  };
}

function remaining() {
  if (!game) return null;

  return Math.max(
    0,
    Math.ceil(
      (Number(game.roundEndsAt) - Date.now()) / 1000
    )
  );
}

function publicState() {
  return {
    roundNumber: game?.roundNumber ?? null,
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    serverTime: Date.now(),
    secondsRemaining: remaining(),
    bettingEnabled: BETTING_ENABLED && firebaseReady,
    phase: BETTING_ENABLED
      ? "betting"
      : "betting_disabled",
    multipliers: MULTIPLIERS,
    history: historyArray(game?.history)
  };
}

function sendState() {
  if (game) {
    io.to(GAME_ROOM).emit(
      "greedy_lion_state",
      publicState()
    );
  }
}

function sendClock() {
  if (!game) return;

  io.to(GAME_ROOM).emit("greedy_lion_clock", {
    roundNumber: game.roundNumber,
    roundStartsAt: game.roundStartsAt,
    roundEndsAt: game.roundEndsAt,
    serverTime: Date.now(),
    secondsRemaining: remaining()
  });
}

async function loadGame() {
  if (!firebaseReady || loading) return;

  loading = true;

  try {
    let saved = (await gameRef().once("value")).val();

    if (saved === null) {
      const now = Date.now();

      const tx = await gameRef().transaction(
        current => current === null
          ? createGame(now)
          : undefined,
        undefined,
        false
      );

      saved = tx.snapshot.val();

      if (!saved) {
        saved = (await gameRef().once("value")).val();
      }
    }

    if (!validGame(saved)) {
      console.error("INVALID GAME DATA");
      return;
    }

    game = saved;
    lastSync = Date.now();

    console.log("GAME LOADED:", game.roundNumber);
    sendState();
  } catch (error) {
    console.error("LOAD ERROR:", error.message);
  } finally {
    loading = false;
  }
}

async function advanceGame() {
  if (!firebaseReady || roundProcessing) return;

  roundProcessing = true;

  try {
    const now = Date.now();
    const winner = crypto.randomInt(8);

    const tx = await gameRef().transaction(
      current => {
        if (current === null) {
          return createGame(now);
        }

        if (!validGame(current)) return;

        const start = Number(current.roundStartsAt);
        const end = Number(current.roundEndsAt);

        if (
          end - start !== ROUND_MS ||
          start > now + ROUND_MS
        ) {
          return {
            ...current,
            roundStartsAt: now,
            roundEndsAt: now + ROUND_MS
          };
        }

        if (Date.now() < end) return;

        const roundNumber = current.roundNumber;
        const activeBets = current.activeBets || {};
        const pending = {
          ...(current.pendingSettlement || {})
        };

        const result = {
          roundNumber,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        if (Object.keys(activeBets).length) {
          pending[String(roundNumber)] = {
            roundNumber,
            winningIndex: winner,
            completedAt: now,
            payoutVersion: 2,
            settled: false,
            bets: activeBets
          };
        }

        return {
          ...current,
          roundNumber: roundNumber + 1,
          roundStartsAt: now,
          roundEndsAt: now + ROUND_MS,
          activeBets: {},
          pendingSettlement: pending,
          history: [
            ...historyArray(current.history),
            result
          ].slice(-8)
        };
      },
      undefined,
      false
    );

    const saved = tx.snapshot.val();

    if (!validGame(saved)) return;

    const previous = game?.roundNumber;
    game = saved;
    lastSync = Date.now();

    if (
      previous != null &&
      game.roundNumber > previous
    ) {
      const completed = game.roundNumber - 1;
      const result = historyArray(game.history)
        .find(x => x.roundNumber === completed);

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      console.log("ROUND FINISHED:", completed);
    }

    sendState();
    sendClock();
  } catch (error) {
    console.error("ROUND ERROR:", error.message);
  } finally {
    roundProcessing = false;
  }
}

/*
  WALLET TRANSACTIONS

  A bet is first reserved in the user's wallet.
  Then it is recorded in the game.

  If the game rejects it, the reservation is
  refunded using the same unique bet ID.
*/

async function reserveCoins(uid, betId, amount, roundNumber) {
  const ref = userRef(uid);
  let reason = "WALLET_REJECTED";

  const tx = await ref.transaction(
    user => {
      if (!user) {
        reason = "USER_NOT_FOUND";
        return;
      }

      const bets = user.lionBets || {};

      if (bets[betId]) {
        reason = "DUPLICATE_BET";
        return;
      }

      const coins = Number(user.coins);

      if (
        !Number.isSafeInteger(coins) ||
        coins < amount
      ) {
        reason = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...user,
        coins: coins - amount,
        lionBets: {
          ...bets,
          [betId]: {
            amount,
            roundNumber,
            status: "reserved",
            createdAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  return {
    ok: tx.committed,
    error: tx.committed ? null : reason,
    balance: tx.snapshot.val()?.coins
  };
}

async function setBetStatus(uid, betId, status) {
  const ref = userRef(uid);

  const tx = await ref.transaction(
    user => {
      if (!user?.lionBets?.[betId]) return;

      const bet = user.lionBets[betId];

      if (bet.status === status) return;

      if (bet.status !== "reserved") return;

      return {
        ...user,
        lionBets: {
          ...user.lionBets,
          [betId]: {
            ...bet,
            status,
            updatedAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  return tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]?.status === status;
}

async function refundBet(uid, betId) {
  const tx = await userRef(uid).transaction(
    user => {
      if (!user?.lionBets?.[betId]) return;

      const bet = user.lionBets[betId];

      if (bet.status === "refunded") return;

      if (bet.status !== "reserved") return;

      const coins = Number(user.coins);
      const amount = Number(bet.amount);

      if (
        !Number.isSafeInteger(coins) ||
        !Number.isSafeInteger(amount) ||
        !Number.isSafeInteger(coins + amount)
      ) {
        return;
      }

      return {
        ...user,
        coins: coins + amount,
        lionBets: {
          ...user.lionBets,
          [betId]: {
            ...bet,
            status: "refunded",
            refundedAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  return tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]?.status ===
      "refunded";
}

async function placeBet(data) {
  if (!BETTING_ENABLED) {
    return {
      ok: false,
      error: "BETTING_DISABLED"
    };
  }

  if (!firebaseReady || !game) {
    return {
      ok: false,
      error: "SERVER_NOT_READY"
    };
  }

  const token = String(data?.idToken || "");
  const betId = String(data?.betId || "");

  const foodIndex = Number(data?.foodIndex);
  const amount = Number(data?.amount);
  const roundNumber = Number(data?.roundNumber);

  if (
    !/^[a-f0-9]{32}$/i.test(betId) ||
    !Number.isInteger(foodIndex) ||
    foodIndex < 0 ||
    foodIndex > 7 ||
    !CHIPS.includes(amount) ||
    !Number.isSafeInteger(roundNumber)
  ) {
    return {
      ok: false,
      error: "INVALID_BET"
    };
  }

  let decoded;

  try {
    decoded = await admin.auth().verifyIdToken(token);
  } catch {
    return {
      ok: false,
      error: "INVALID_TOKEN"
    };
  }

  const uid = decoded.uid;
  const betKey = `${uid}_${betId}`;

  const currentGame =
    (await gameRef().once("value")).val();

  if (
    !validGame(currentGame) ||
    currentGame.roundNumber !== roundNumber ||
    Date.now() >= currentGame.roundEndsAt
  ) {
    return {
      ok: false,
      error: "ROUND_CLOSED"
    };
  }

  let reserved;

  try {
    reserved = await reserveCoins(
      uid,
      betId,
      amount,
      roundNumber
    );
  } catch (error) {
    console.error("RESERVE ERROR:", error.message);

    return {
      ok: false,
      error: "WALLET_ERROR"
    };
  }

  if (!reserved.ok) {
    return {
      ok: false,
      error: reserved.error
    };
  }

  let rejection = "BET_REJECTED";

  try {
    const tx = await gameRef().transaction(
      current => {
        if (!validGame(current)) {
          rejection = "GAME_NOT_READY";
          return;
        }

        if (
          current.roundNumber !== roundNumber ||
          Date.now() >= current.roundEndsAt
        ) {
          rejection = "ROUND_CLOSED";
          return;
        }

        const activeBets = {
          ...(current.activeBets || {})
        };

        if (activeBets[betKey]) {
          rejection = "DUPLICATE_BET";
          return;
        }

        const selected = new Set();

        for (const bet of Object.values(activeBets)) {
          if (bet.uid === uid) {
            selected.add(Number(bet.foodIndex));
          }
        }

        if (
          !selected.has(foodIndex) &&
          selected.size >= 6
        ) {
          rejection = "MAX_SIX_SLOTS";
          return;
        }

        activeBets[betKey] = {
          uid,
          betId,
          foodIndex,
          amount,
          roundNumber,
          createdAt: Date.now()
        };

        return {
          ...current,
          activeBets
        };
      },
      undefined,
      false
    );

    if (!tx.committed) {
      const refunded = await refundBet(uid, betId);

      return {
        ok: false,
        error: refunded
          ? rejection
          : "REFUND_PENDING"
      };
    }

    // A committed game bet must not be refunded.
    // If marking the wallet fails, the game bet
    // still exists and can be reconciled later.
    try {
      await setBetStatus(uid, betId, "accepted");
    } catch (error) {
      console.error(
        "BET STATUS ERROR:",
        error.message
      );
    }

    game = tx.snapshot.val();
    lastSync = Date.now();

    console.log(
      "BET ACCEPTED:",
      uid,
      amount,
      foodIndex
    );

    return {
      ok: true,
      betId,
      foodIndex,
      amount,
      roundNumber,
      balance: reserved.balance
    };
  } catch (error) {
    console.error("BET ERROR:", error.message);

    // The game transaction may have committed
    // before a network error. Check before refund.
    try {
      const snap = await gameRef().once("value");
      const saved = snap.val();

      const inActive = Boolean(
        saved?.activeBets?.[betKey]
      );

      const inPending = Object.values(
        saved?.pendingSettlement || {}
      ).some(entry =>
        Boolean(entry?.bets?.[betKey])
      );

      if (inActive || inPending) {
        return {
          ok: true,
          betId,
          foodIndex,
          amount,
          roundNumber,
          balance: reserved.balance
        };
      }

      const refunded = await refundBet(uid, betId);

      return {
        ok: false,
        error: refunded
          ? "BET_FAILED_REFUNDED"
          : "BET_STATUS_UNCERTAIN"
      };
    } catch {
      return {
        ok: false,
        error: "BET_STATUS_UNCERTAIN"
      };
    }
  }
}

/*
  PAYOUTS

  The payout marker is written in the same
  user transaction as the coin credit.
*/

function calculatePayouts(entry) {
  const totals = {};

  const winner = Number(entry.winningIndex);

  for (const bet of Object.values(entry.bets || {})) {
    const index = Number(bet.foodIndex);
    const amount = Number(bet.amount);
    const uid = bet.uid;

    if (
      typeof uid !== "string" ||
      !uid ||
      index !== winner ||
      !Number.isSafeInteger(amount) ||
      amount <= 0
    ) {
      continue;
    }

    const payout = amount * MULTIPLIERS[index];

    if (!Number.isSafeInteger(payout)) continue;

    totals[uid] = (totals[uid] || 0) + payout;
  }

  return totals;
}

async function creditWinner(uid, roundNumber, amount) {
  const key = String(roundNumber);

  const tx = await userRef(uid).transaction(
    user => {
      if (!user) return;

      const payouts = user.lionPayouts || {};

      if (payouts[key]) return;

      const coins = Number(user.coins);

      if (
        !Number.isSafeInteger(coins) ||
        coins < 0 ||
        !Number.isSafeInteger(amount) ||
        amount <= 0 ||
        !Number.isSafeInteger(coins + amount)
      ) {
        return;
      }

      return {
        ...user,
        coins: coins + amount,
        lionPayouts: {
          ...payouts,
          [key]: {
            amount,
            completedAt: Date.now()
          }
        }
      };
    },
    undefined,
    false
  );

  if (tx.committed) return true;

  return Boolean(
    tx.snapshot.val()?.lionPayouts?.[key]
  );
}

async function settlePending() {
  if (
    !firebaseReady ||
    !game ||
    settlementProcessing
  ) {
    return;
  }

  settlementProcessing = true;

  try {
    const snapshot = await gameRef()
      .child("pendingSettlement")
      .once("value");

    const pending = snapshot.val() || {};

    for (const [key, entry] of Object.entries(pending)) {
      if (!entry || entry.settled) continue;

      // Old settlement records are preserved.
      if (
        entry.payoutVersion !== 1 &&
        entry.payoutVersion !== 2
      ) {
        continue;
      }

      const totals = calculatePayouts(entry);
      let allPaid = true;

      for (const [uid, amount] of Object.entries(totals)) {
        try {
          const paid = await creditWinner(
            uid,
            entry.roundNumber,
            amount
          );

          if (!paid) allPaid = false;
        } catch (error) {
          allPaid = false;

          console.error(
            "PAYOUT ERROR:",
            error.message
          );
        }
      }

      if (!allPaid) continue;

      const ref = gameRef()
        .child("pendingSettlement")
        .child(key);

      await ref.update({
        settled: true,
        settledAt: Date.now()
      });

      console.log("ROUND SETTLED:", key);
    }
  } catch (error) {
    console.error(
      "SETTLEMENT ERROR:",
      error.message
    );
  } finally {
    settlementProcessing = false;
  }
}

/*
  INDEPENDENT CLOCK
*/

setInterval(() => {
  sendClock();

  if (!firebaseReady) return;

  if (!game) {
    if (!loading) loadGame().catch(console.error);
    return;
  }

  const now = Date.now();

  const duration =
    Number(game.roundEndsAt) -
    Number(game.roundStartsAt);

  const needsUpdate =
    now >= Number(game.roundEndsAt) ||
    duration !== ROUND_MS ||
    Number(game.roundStartsAt) > now + ROUND_MS;

  const needsSync = now - lastSync >= 10000;

  if (
    (needsUpdate || needsSync) &&
    !roundProcessing
  ) {
    advanceGame().catch(console.error);
  }
}, 1000);

setInterval(() => {
  if (!settlementProcessing) {
    settlePending().catch(console.error);
  }
}, 5000);

/*
  VOICE AND GAME SOCKET EVENTS
*/

io.on("connection", socket => {
  console.log("USER CONNECTED:", socket.id);

  socket.on("join_room", data => {
    if (!data?.roomId) return;

    socket.join(data.roomId);

    socket.to(data.roomId).emit(
      "user_joined_room",
      {
        userId: data.userId,
        userName: data.userName
      }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data?.roomId) return;

    io.to(data.roomId).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data?.roomId) return;

    io.to(data.roomId).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data?.roomId) return;

    io.to(data.roomId).emit(
      "user_left_seat",
      {
        userId: data.userId
      }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(data.roomId).emit(
      "user_started_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(data.roomId).emit(
      "user_stopped_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("voice_data", data => {
    if (!data?.roomId) return;

    socket.to(data.roomId).emit(
      "voice_data",
      {
        audioBuffer: data.audioBuffer,
        userId: data.userId
      }
    );
  });

  socket.on("greedy_lion_join", async () => {
    socket.join(GAME_ROOM);

    if (!game) await loadGame();

    if (game && socket.connected) {
      socket.emit(
        "greedy_lion_state",
        publicState()
      );

      socket.emit(
        "greedy_lion_clock",
        {
          roundNumber: game.roundNumber,
          roundStartsAt: game.roundStartsAt,
          roundEndsAt: game.roundEndsAt,
          serverTime: Date.now(),
          secondsRemaining: remaining()
        }
      );
    }
  });

  socket.on(
    "greedy_lion_place_bet",
    async (data, ack) => {
      const reply =
        typeof ack === "function"
          ? ack
          : () => {};

      try {
        reply(await placeBet(data));
      } catch (error) {
        console.error(
          "SOCKET BET ERROR:",
          error.message
        );

        reply({
          ok: false,
          error: "SERVER_ERROR"
        });
      }
    }
  );

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log("USER DISCONNECTED:", socket.id);
  });
});

/*
  SERVER STATUS
*/

app.get("/", (req, res) => {
  const history = historyArray(game?.history);

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: BETTING_ENABLED,
    roundNumber: game?.roundNumber ?? null,
    secondsRemaining: remaining(),
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    durationMs: game
      ? Number(game.roundEndsAt) -
        Number(game.roundStartsAt)
      : null,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

server.listen(PORT, async () => {
  console.log("MAX CHAT SERVER RUNNING:", PORT);
  console.log("BETTING ENABLED:", BETTING_ENABLED);

  await loadGame();

  if (game) await advanceGame();

  console.log(
    "GAME READY:",
    game?.roundNumber ?? "NOT READY"
  );
});
