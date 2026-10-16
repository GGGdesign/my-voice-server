
const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const { Server } = require("socket.io");
const admin = require("firebase-admin");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const DATABASE_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GAME_ROOM = "maxchat_greedy_lion_global";
const GAME_PATH = "greedyLionGame";

const ROUND_MS = 25000;
const FOOD_MULTIPLIERS = [
  45, 5, 5, 10, 15, 5, 5, 25
];

const ALLOWED_CHIPS = [
  100, 500, 1000, 5000, 10000
];

const MAX_SLOTS = 6;

// Keep disabled until Android integration is tested.
const BETTING_ENABLED = false;

let firebaseReady = false;
let gameState = null;
let clockBusy = false;

try {
  if (fs.existsSync(SECRET_PATH)) {
    const credentials = JSON.parse(
      fs.readFileSync(SECRET_PATH, "utf8")
    );

    admin.initializeApp({
      credential: admin.credential.cert(credentials),
      databaseURL: DATABASE_URL
    });

    firebaseReady = true;
    console.log("Firebase Admin ready");
  } else {
    console.error("Firebase secret missing");
  }
} catch (error) {
  console.error(
    "Firebase initialization error:",
    error.message
  );
}

function initialState() {
  const now = Date.now();

  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: [],
    bets: {}
  };
}

function validState(state) {
  return Boolean(
    state &&
    Number.isSafeInteger(state.roundNumber) &&
    state.roundNumber > 0 &&
    Number.isFinite(state.roundStartsAt) &&
    Number.isFinite(state.roundEndsAt) &&
    state.roundEndsAt > state.roundStartsAt
  );
}

function historyArray(value) {
  if (Array.isArray(value)) return value;

  if (value && typeof value === "object") {
    return Object.keys(value)
      .sort((a, b) => Number(a) - Number(b))
      .map(key => value[key]);
  }

  return [];
}

function publicState() {
  if (!gameState) return null;

  return {
    roundNumber: gameState.roundNumber,
    roundStartsAt: gameState.roundStartsAt,
    roundEndsAt: gameState.roundEndsAt,
    serverTime: Date.now(),
    phase: BETTING_ENABLED
      ? "betting"
      : "betting_disabled",
    bettingEnabled:
      BETTING_ENABLED && firebaseReady,
    multipliers: FOOD_MULTIPLIERS,
    history: historyArray(
      gameState.history
    ).slice(-8)
  };
}

function broadcastState() {
  const state = publicState();

  if (state) {
    io.to(GAME_ROOM).emit(
      "greedy_lion_state",
      state
    );
  }
}

function broadcastClock() {
  if (!gameState) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_clock",
    {
      roundNumber: gameState.roundNumber,
      roundEndsAt: gameState.roundEndsAt,
      serverTime: Date.now()
    }
  );
}

async function loadGame() {
  if (!firebaseReady) {
    gameState = initialState();
    return;
  }

  const ref = admin.database().ref(GAME_PATH);

  const result = await ref.transaction(
    current => {
      if (validState(current)) {
        return current;
      }

      return initialState();
    },
    undefined,
    false
  );

  gameState = result.snapshot.val();

  console.log(
    "Game loaded:",
    gameState.roundNumber
  );
}

async function finishRound() {
  if (!gameState) return;

  if (Date.now() < gameState.roundEndsAt) {
    return;
  }

  // Offline fallback is for visual testing only.
  if (!firebaseReady) {
    const now = Date.now();

    const result = {
      roundNumber: gameState.roundNumber,
      winningIndex: crypto.randomInt(8),
      completedAt: now,
      hasSettledBets: false
    };

    gameState.history = [
      ...historyArray(gameState.history),
      result
    ].slice(-8);

    gameState.roundNumber += 1;
    gameState.roundStartsAt = now;
    gameState.roundEndsAt = now + ROUND_MS;

    io.to(GAME_ROOM).emit(
      "greedy_lion_round_result",
      result
    );

    broadcastState();
    return;
  }

  const winner = crypto.randomInt(8);
  const now = Date.now();

  let finished = null;

  // One atomic transaction for the game and
  // every affected wallet. Never pay twice.
  const tx = await admin.database()
    .ref("/")
    .transaction(root => {
      if (!root || typeof root !== "object") {
        return;
      }

      const game = root[GAME_PATH];

      if (!validState(game)) return;

      if (Date.now() < game.roundEndsAt) {
        return;
      }

      const bets = game.bets || {};
      const roundNumber = game.roundNumber;

      const payouts = {};

      for (const bet of Object.values(bets)) {
        if (!bet || !bet.uid) continue;

        if (bet.foodIndex !== winner) {
          continue;
        }

        const amount = Number(bet.amount);

        if (!ALLOWED_CHIPS.includes(amount)) {
          return;
        }

        const payout =
          amount * FOOD_MULTIPLIERS[winner];

        payouts[bet.uid] =
          (payouts[bet.uid] || 0) +
          payout;
      }

      for (const [uid, payout] of
        Object.entries(payouts)) {

        const user = root.Users?.[uid];

        if (!user) return;

        const balance = Number(user.coins);

        if (
          !Number.isSafeInteger(balance) ||
          balance < 0 ||
          !Number.isSafeInteger(payout) ||
          !Number.isSafeInteger(balance + payout)
        ) {
          return;
        }

        user.coins = balance + payout;
      }

      const result = {
        roundNumber,
        winningIndex: winner,
        completedAt: now,
        hasSettledBets: true
      };

      game.history = [
        ...historyArray(game.history),
        result
      ].slice(-8);

      game.roundNumber = roundNumber + 1;
      game.roundStartsAt = now;
      game.roundEndsAt = now + ROUND_MS;
      game.bets = {};

      root[GAME_PATH] = game;

      return root;
    }, undefined, false);

  if (!tx.committed) {
    return;
  }

  const updated = tx.snapshot
    .child(GAME_PATH)
    .val();

  if (!validState(updated)) return;

  gameState = updated;

  finished = historyArray(
    updated.history
  ).slice(-1)[0];

  if (finished) {
    io.to(GAME_ROOM).emit(
      "greedy_lion_round_result",
      finished
    );
  }

  console.log(
    "ROUND FINISHED:",
    finished?.roundNumber,
    "WINNER:",
    finished?.winningIndex
  );

  console.log(
    "ROUND STARTED:",
    gameState.roundNumber
  );

  broadcastState();
}

async function placeBet(data, ack) {
  const reply =
    typeof ack === "function"
      ? ack
      : () => {};

  if (!BETTING_ENABLED || !firebaseReady) {
    reply({
      ok: false,
      error: "BETTING_DISABLED",
      message:
        "الرهانات مغلقة مؤقتًا لحماية الرصيد"
    });
    return;
  }

  if (!gameState) {
    reply({
      ok: false,
      error: "GAME_NOT_READY"
    });
    return;
  }

  try {
    const token = data?.idToken;
    const betId = data?.betId;
    const foodIndex = data?.foodIndex;
    const amount = data?.amount;
    const requestedRound = data?.roundNumber;

    if (
      typeof token !== "string" ||
      !/^[a-zA-Z0-9_-]{8,100}$/.test(
        String(betId || "")
      ) ||
      !Number.isInteger(foodIndex) ||
      foodIndex < 0 ||
      foodIndex >= 8 ||
      !ALLOWED_CHIPS.includes(amount) ||
      !Number.isSafeInteger(requestedRound)
    ) {
      reply({
        ok: false,
        error: "INVALID_BET"
      });
      return;
    }

    // Never trust a UID sent by the phone.
    const verified = await admin.auth()
      .verifyIdToken(token, true);

    const uid = verified.uid;

    let rejection = "BET_REJECTED";

    const tx = await admin.database()
      .ref("/")
      .transaction(root => {
        if (!root || typeof root !== "object") {
          rejection = "DATABASE_NOT_READY";
          return;
        }

        const game = root[GAME_PATH];
        const user = root.Users?.[uid];

        if (!validState(game) || !user) {
          rejection = "GAME_OR_USER_MISSING";
          return;
        }

        if (
          game.roundNumber !== requestedRound ||
          Date.now() >= game.roundEndsAt
        ) {
          rejection = "ROUND_CLOSED";
          return;
        }

        if (!game.bets) {
          game.bets = {};
        }

        const key = uid + "_" + betId;

        // Duplicate request cannot debit twice.
        if (game.bets[key]) {
          rejection = "DUPLICATE_BET";
          return;
        }

        const userBets = Object.values(
          game.bets
        ).filter(bet => bet.uid === uid);

        const chosenSlots = new Set(
          userBets.map(bet => bet.foodIndex)
        );

        if (
          !chosenSlots.has(foodIndex) &&
          chosenSlots.size >= MAX_SLOTS
        ) {
          rejection = "MAX_SIX_SLOTS";
          return;
        }

        const balance = Number(user.coins);

        if (
          !Number.isSafeInteger(balance) ||
          balance < amount
        ) {
          rejection = "INSUFFICIENT_COINS";
          return;
        }

        user.coins = balance - amount;

        game.bets[key] = {
          uid,
          betId,
          foodIndex,
          amount,
          roundNumber: requestedRound,
          createdAt: Date.now()
        };

        return root;
      }, undefined, false);

    if (!tx.committed) {
      reply({
        ok: false,
        error: rejection
      });
      return;
    }

    gameState = tx.snapshot
      .child(GAME_PATH)
      .val();

    reply({
      ok: true,
      betId,
      foodIndex,
      amount,
      roundNumber: requestedRound
    });

    console.log(
      "BET ACCEPTED:",
      requestedRound,
      foodIndex,
      amount
    );

  } catch (error) {
    console.error(
      "BET ERROR:",
      error.message
    );

    reply({
      ok: false,
      error: "BET_FAILED"
    });
  }
}

setInterval(async () => {
  if (clockBusy || !gameState) return;

  clockBusy = true;

  try {
    await finishRound();
    broadcastClock();
  } catch (error) {
    console.error(
      "GAME CLOCK ERROR:",
      error.message
    );
  } finally {
    clockBusy = false;
  }
}, 1000);

io.on("connection", socket => {
  console.log("User connected:", socket.id);

  // Existing voice chat events

  socket.on("join_room", data => {
    if (!data?.roomId) return;

    const { roomId, userId, userName } = data;

    socket.join(roomId);

    socket.to(roomId).emit(
      "user_joined_room",
      { userId, userName }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data?.roomId) return;

    const { roomId, userId, isMuted } = data;

    io.to(roomId).emit(
      "user_toggle_mute",
      { userId, isMuted }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data?.roomId) return;

    const { roomId, userId, seatIndex } = data;

    io.to(roomId).emit(
      "user_sat_on_seat",
      { userId, seatIndex }
    );
  });

  socket.on("leave_seat", data => {
    if (!data?.roomId) return;

    const { roomId, userId } = data;

    io.to(roomId).emit(
      "user_left_seat",
      { roomId, userId }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    const { roomId, userId } = data;

    socket.to(roomId).emit(
      "user_started_audio",
      { roomId, userId }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    const { roomId, userId } = data;

    socket.to(roomId).emit(
      "user_stopped_audio",
      { roomId, userId }
    );
  });

  socket.on("voice_data", data => {
    if (!data?.roomId) return;

    const { roomId, audioBuffer, userId } = data;

    socket.to(roomId).emit(
      "voice_data",
      { roomId, audioBuffer, userId }
    );
  });

  // Greedy Lion

  socket.on("greedy_lion_join", () => {
    socket.join(GAME_ROOM);

    const state = publicState();

    if (state) {
      socket.emit(
        "greedy_lion_state",
        state
      );
    }

    if (gameState) {
      socket.emit(
        "greedy_lion_clock",
        {
          roundNumber: gameState.roundNumber,
          roundEndsAt: gameState.roundEndsAt,
          serverTime: Date.now()
        }
      );
    }
  });

  socket.on(
    "greedy_lion_place_bet",
    (data, ack) => {
      placeBet(data, ack);
    }
  );

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log(
      "User disconnected:",
      socket.id
    );
  });
});

app.get("/", (req, res) => {
  res.set("Cache-Control", "no-store");

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled:
      BETTING_ENABLED && firebaseReady,
    roundNumber:
      gameState?.roundNumber ?? null,
    secondsRemaining: gameState
      ? Math.max(
          0,
          Math.ceil(
            (gameState.roundEndsAt - Date.now()) /
            1000
          )
        )
      : null,
    lastResult: gameState
      ? historyArray(
          gameState.history
        ).slice(-1)[0] || null
      : null
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, async () => {
  console.log(
    "Max Chat server running on port",
    PORT
  );

  try {
    await loadGame();

    console.log(
      "GAME READY:",
      gameState.roundNumber
    );
  } catch (error) {
    console.error(
      "GAME STARTUP ERROR:",
      error.message
    );
  }
});
