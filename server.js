
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

// Keep real wallet betting disabled.
const BETTING_ENABLED = false;

let firebaseReady = false;
let gameState = null;
let processingRound = false;
let clockBusy = false;

// ---------------------------------------
// Firebase Admin
// ---------------------------------------

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

function database() {
  return admin.database();
}

// ---------------------------------------
// Game state
// ---------------------------------------

function initialState() {
  const now = Date.now();

  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: []
  };
}

// FIX:
// Firebase removes empty arrays.
// Missing history is a valid empty history.
function validState(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber >= 1 &&
    Number.isFinite(value.roundStartsAt) &&
    Number.isFinite(value.roundEndsAt) &&
    value.roundEndsAt > value.roundStartsAt &&
    (
      value.history === undefined ||
      value.history === null ||
      Array.isArray(value.history)
    )
  );
}

function getHistory(state) {
  if (!state || !Array.isArray(state.history)) {
    return [];
  }

  return state.history;
}

function publicState() {
  if (!gameState) {
    return null;
  }

  return {
    roundNumber: gameState.roundNumber,
    roundStartsAt: gameState.roundStartsAt,
    roundEndsAt: gameState.roundEndsAt,
    serverTime: Date.now(),
    phase: "betting_disabled",
    bettingEnabled: BETTING_ENABLED,
    history: getHistory(gameState).slice(-8),
    multipliers: FOOD_MULTIPLIERS
  };
}

function broadcastState() {
  const state = publicState();

  if (!state) {
    return;
  }

  io.to(GAME_ROOM).emit(
    "greedy_lion_state",
    state
  );
}

function broadcastClock() {
  if (!gameState) {
    return;
  }

  io.to(GAME_ROOM).emit(
    "greedy_lion_clock",
    {
      roundNumber: gameState.roundNumber,
      roundEndsAt: gameState.roundEndsAt,
      serverTime: Date.now()
    }
  );
}

// ---------------------------------------
// Initialize saved game
// ---------------------------------------

async function initializeGame() {
  if (!firebaseReady) {
    console.error(
      "Game paused: Firebase unavailable"
    );
    return;
  }

  const ref = database().ref(GAME_PATH);

  const tx = await ref.transaction(
    current => {
      if (validState(current)) {
        return;
      }

      if (current === null) {
        return initialState();
      }

      // Never erase unknown existing data.
      console.error(
        "Invalid game data. No data deleted."
      );

      return;
    },
    undefined,
    false
  );

  const loaded = tx.snapshot.val();

  if (!validState(loaded)) {
    throw new Error(
      "Invalid game state in Firebase"
    );
  }

  gameState = loaded;

  console.log(
    "Greedy Lion loaded. Round:",
    gameState.roundNumber
  );

  console.log(
    "ROUND STARTED:",
    gameState.roundNumber
  );

  broadcastState();
  broadcastClock();

  // Handle an expired round from before restart.
  if (Date.now() >= gameState.roundEndsAt) {
    await finishRound();
  }
}

// ---------------------------------------
// Complete round
// ---------------------------------------

async function finishRound() {
  if (
    processingRound ||
    !firebaseReady ||
    !gameState
  ) {
    return;
  }

  if (Date.now() < gameState.roundEndsAt) {
    return;
  }

  processingRound = true;

  try {
    const ref = database().ref(GAME_PATH);

    const snapshot = await ref.once("value");
    const latest = snapshot.val();

    if (!validState(latest)) {
      throw new Error(
        "Invalid game state during round"
      );
    }

    gameState = latest;

    if (Date.now() < latest.roundEndsAt) {
      broadcastState();
      return;
    }

    const oldRound = latest.roundNumber;

    // Choose once for this transition.
    const winner = crypto.randomInt(8);

    const tx = await ref.transaction(
      current => {
        if (!validState(current)) {
          return;
        }

        if (
          current.roundNumber !== oldRound ||
          Date.now() < current.roundEndsAt
        ) {
          return;
        }

        const now = Date.now();

        const roundResult = {
          roundNumber: oldRound,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        const previousHistory =
          getHistory(current);

        current.history = [
          ...previousHistory,
          roundResult
        ].slice(-8);

        current.roundNumber =
          oldRound + 1;

        current.roundStartsAt = now;
        current.roundEndsAt =
          now + ROUND_MS;

        return current;
      },
      undefined,
      false
    );

    const saved = tx.snapshot.val();

    if (!validState(saved)) {
      throw new Error(
        "Invalid saved game state"
      );
    }

    gameState = saved;

    if (tx.committed) {
      const result = getHistory(saved).find(
        item => item.roundNumber === oldRound
      );

      if (result) {
        console.log(
          "ROUND FINISHED:",
          oldRound,
          "WINNER:",
          result.winningIndex
        );

        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      console.log(
        "ROUND STARTED:",
        saved.roundNumber
      );
    }

    broadcastState();
    broadcastClock();

  } catch (error) {
    console.error(
      "ROUND ERROR:",
      error.message
    );
  } finally {
    processingRound = false;
  }
}

// ---------------------------------------
// Clock - every second
// ---------------------------------------

async function gameTick() {
  if (clockBusy || !gameState) {
    return;
  }

  clockBusy = true;

  try {
    broadcastClock();

    if (Date.now() >= gameState.roundEndsAt) {
      await finishRound();
    }
  } finally {
    clockBusy = false;
  }
}

setInterval(() => {
  gameTick().catch(error => {
    console.error(
      "GAME TICK ERROR:",
      error.message
    );
  });
}, 1000);

// ---------------------------------------
// Socket.IO
// ---------------------------------------

io.on("connection", socket => {
  console.log(
    "User connected:",
    socket.id
  );

  // Existing voice chat events

  socket.on("join_room", data => {
    if (!data || !data.roomId) return;

    const {
      roomId,
      userId,
      userName
    } = data;

    socket.join(roomId);

    socket.to(roomId).emit(
      "user_joined_room",
      {
        userId,
        userName
      }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data || !data.roomId) return;

    const {
      roomId,
      userId,
      isMuted
    } = data;

    io.to(roomId).emit(
      "user_toggle_mute",
      {
        userId,
        isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data || !data.roomId) return;

    const {
      roomId,
      userId,
      seatIndex
    } = data;

    io.to(roomId).emit(
      "user_sat_on_seat",
      {
        userId,
        seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data || !data.roomId) return;

    const {
      roomId,
      userId
    } = data;

    io.to(roomId).emit(
      "user_left_seat",
      {
        userId
      }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data || !data.roomId) return;

    const {
      roomId,
      userId
    } = data;

    socket.to(roomId).emit(
      "user_started_audio",
      {
        userId
      }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data || !data.roomId) return;

    const {
      roomId,
      userId
    } = data;

    socket.to(roomId).emit(
      "user_stopped_audio",
      {
        userId
      }
    );
  });

  socket.on("voice_data", data => {
    if (!data || !data.roomId) return;

    const {
      roomId,
      audioBuffer,
      userId
    } = data;

    socket.to(roomId).emit(
      "voice_data",
      {
        roomId,
        audioBuffer,
        userId
      }
    );
  });

  // -------------------------------------
  // Greedy Lion events
  // -------------------------------------

  socket.on("greedy_lion_join", async () => {
    socket.join(GAME_ROOM);

    try {
      if (firebaseReady) {
        const snapshot = await database()
          .ref(GAME_PATH)
          .once("value");

        const latest = snapshot.val();

        if (validState(latest)) {
          gameState = latest;
        }
      }

      if (gameState) {
        socket.emit(
          "greedy_lion_state",
          publicState()
        );

        socket.emit(
          "greedy_lion_clock",
          {
            roundNumber: gameState.roundNumber,
            roundEndsAt: gameState.roundEndsAt,
            serverTime: Date.now()
          }
        );
      }
    } catch (error) {
      console.error(
        "JOIN ERROR:",
        error.message
      );
    }
  });

  // Betting stays locked.
  socket.on(
    "greedy_lion_place_bet",
    (data, ack) => {
      if (typeof ack === "function") {
        ack({
          ok: false,
          error: "BETTING_DISABLED",
          message:
            "الرهانات مغلقة مؤقتًا لحماية الرصيد"
        });
      }
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

// ---------------------------------------
// Health check
// ---------------------------------------

app.get("/", (req, res) => {
  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: BETTING_ENABLED,
    roundNumber:
      gameState?.roundNumber ?? null,
    secondsRemaining: gameState
      ? Math.max(
          0,
          Math.ceil(
            (
              gameState.roundEndsAt -
              Date.now()
            ) / 1000
          )
        )
      : null,
    lastResult:
      getHistory(gameState).slice(-1)[0] ??
      null
  });
});

// ---------------------------------------
// Start server
// ---------------------------------------

const PORT = process.env.PORT || 10000;

server.listen(PORT, async () => {
  console.log(
    "Max Chat server running on port",
    PORT
  );

  try {
    await initializeGame();
  } catch (error) {
    console.error(
      "GAME STARTUP ERROR:",
      error.message
    );
  }
});
