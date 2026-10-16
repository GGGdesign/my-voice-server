
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

// Real wallet betting remains disabled.
const BETTING_ENABLED = false;

let firebaseReady = false;
let gameState = null;
let processingRound = false;
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

function database() {
  return admin.database();
}

function initialState() {
  const now = Date.now();

  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: []
  };
}

function validState(value) {
  return value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber >= 1 &&
    Number.isFinite(value.roundEndsAt) &&
    Number.isFinite(value.roundStartsAt) &&
    Array.isArray(value.history);
}

function publicState() {
  const state = gameState || initialState();

  return {
    roundNumber: state.roundNumber,
    roundStartsAt: state.roundStartsAt,
    roundEndsAt: state.roundEndsAt,
    serverTime: Date.now(),
    phase: "betting_disabled",
    bettingEnabled: false,
    history: state.history.slice(-8),
    multipliers: FOOD_MULTIPLIERS
  };
}

function broadcastState() {
  if (!gameState) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_state",
    publicState()
  );
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

      console.error(
        "Invalid existing game data. Manual review required."
      );

      return;
    },
    undefined,
    false
  );

  const loaded = tx.snapshot.val();

  if (!validState(loaded)) {
    throw new Error("Invalid game state in Firebase");
  }

  gameState = loaded;

  console.log(
    "Greedy Lion loaded. Round:",
    gameState.roundNumber
  );

  broadcastState();
}

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

    // Refresh before attempting a round transition.
    const snapshot = await ref.once("value");
    const latest = snapshot.val();

    if (!validState(latest)) {
      throw new Error(
        "Game state missing or invalid"
      );
    }

    gameState = latest;

    if (Date.now() < latest.roundEndsAt) {
      return;
    }

    const oldRound = latest.roundNumber;
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

        const result = {
          roundNumber: oldRound,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        current.history = [
          ...current.history,
          result
        ].slice(-8);

        current.roundNumber = oldRound + 1;
        current.roundStartsAt = now;
        current.roundEndsAt = now + ROUND_MS;

        return current;
      },
      undefined,
      false
    );

    const saved = tx.snapshot.val();

    if (!validState(saved)) {
      throw new Error(
        "Round transaction returned invalid state"
      );
    }

    gameState = saved;

    if (tx.committed) {
      const result = saved.history.find(
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
        saved.roundNumber,
        "ENDS:",
        new Date(saved.roundEndsAt).toISOString()
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

async function gameTick() {
  if (clockBusy || !gameState) {
    return;
  }

  clockBusy = true;

  try {
    // Keep the visible timer moving even while
    // Firebase round completion is being processed.
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

io.on("connection", socket => {
  console.log("User connected:", socket.id);

  // Existing voice chat events

  socket.on("join_room", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId, userName } = data;

    socket.join(roomId);

    socket.to(roomId).emit(
      "user_joined_room",
      { userId, userName }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId, isMuted } = data;

    io.to(roomId).emit(
      "user_toggle_mute",
      { userId, isMuted }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId, seatIndex } = data;

    io.to(roomId).emit(
      "user_sat_on_seat",
      { userId, seatIndex }
    );
  });

  socket.on("leave_seat", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId } = data;

    io.to(roomId).emit(
      "user_left_seat",
      { userId }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId } = data;

    socket.to(roomId).emit(
      "user_started_audio",
      { userId }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data || !data.roomId) return;

    const { roomId, userId } = data;

    socket.to(roomId).emit(
      "user_stopped_audio",
      { userId }
    );
  });

  socket.on("voice_data", data => {
    if (!data || !data.roomId) return;

    const { roomId, audioBuffer, userId } = data;

    socket.to(roomId).emit(
      "voice_data",
      { roomId, audioBuffer, userId }
    );
  });

  // Greedy Lion

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
      }
    } catch (error) {
      console.error(
        "JOIN ERROR:",
        error.message
      );
    }
  });

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

app.get("/", (req, res) => {
  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: BETTING_ENABLED,
    roundNumber: gameState?.roundNumber ?? null,
    secondsRemaining: gameState
      ? Math.max(
          0,
          Math.ceil(
            (gameState.roundEndsAt - Date.now()) / 1000
          )
        )
      : null,
    lastResult:
      gameState?.history?.slice(-1)[0] ?? null
  });
});

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
