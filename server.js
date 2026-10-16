
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

// Security lock: real betting remains disabled.
const BETTING_ENABLED = false;

let firebaseReady = false;
let gameState = null;
let processingRound = false;

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

function makeInitialState() {
  const now = Date.now();

  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: []
  };
}

function publicGameState() {
  const state = gameState || makeInitialState();

  return {
    roundNumber: state.roundNumber,
    roundStartsAt: state.roundStartsAt,
    roundEndsAt: state.roundEndsAt,
    serverTime: Date.now(),
    phase: "betting_disabled",
    bettingEnabled: false,
    history: (state.history || []).slice(-8),
    multipliers: FOOD_MULTIPLIERS
  };
}

async function initializeGame() {
  if (!firebaseReady) {
    gameState = makeInitialState();
    console.warn(
      "Game running without Firebase persistence"
    );
    return;
  }

  const ref = database().ref(GAME_PATH);

  const transaction = await ref.transaction(
    current => current || makeInitialState(),
    undefined,
    false
  );

  gameState = transaction.snapshot.val();

  console.log(
    "Greedy Lion loaded. Round:",
    gameState.roundNumber
  );
}

async function finishRound() {
  if (processingRound || !gameState) {
    return;
  }

  if (Date.now() < gameState.roundEndsAt) {
    return;
  }

  processingRound = true;

  try {
    if (!firebaseReady) {
      const oldRound = gameState.roundNumber;
      const winner = crypto.randomInt(8);
      const now = Date.now();

      const result = {
        roundNumber: oldRound,
        winningIndex: winner,
        completedAt: now,
        hasSettledBets: false
      };

      gameState.history = [
        ...(gameState.history || []),
        result
      ].slice(-8);

      gameState.roundNumber++;
      gameState.roundStartsAt = now;
      gameState.roundEndsAt = now + ROUND_MS;

      io.to(GAME_ROOM).emit(
        "greedy_lion_round_result",
        result
      );

      io.to(GAME_ROOM).emit(
        "greedy_lion_state",
        publicGameState()
      );

      return;
    }

    const ref = database().ref(GAME_PATH);

    const result = await ref.transaction(
      current => {
        if (!current) {
          return;
        }

        if (Date.now() < current.roundEndsAt) {
          return;
        }

        const completedRound =
          current.roundNumber;

        const winner = crypto.randomInt(8);
        const now = Date.now();

        const roundResult = {
          roundNumber: completedRound,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        current.history = [
          ...(current.history || []),
          roundResult
        ].slice(-8);

        current.roundNumber =
          completedRound + 1;

        current.roundStartsAt = now;
        current.roundEndsAt =
          now + ROUND_MS;

        return current;
      },
      undefined,
      false
    );

    gameState = result.snapshot.val();

    if (result.committed && gameState) {
      const latest =
        gameState.history[
          gameState.history.length - 1
        ];

      io.to(GAME_ROOM).emit(
        "greedy_lion_round_result",
        latest
      );

      io.to(GAME_ROOM).emit(
        "greedy_lion_state",
        publicGameState()
      );
    }
  } catch (error) {
    console.error(
      "Round completion failed:",
      error.message
    );
  } finally {
    processingRound = false;
  }
}

async function refreshGame() {
  try {
    if (firebaseReady) {
      const snapshot = await database()
        .ref(GAME_PATH)
        .once("value");

      if (snapshot.exists()) {
        gameState = snapshot.val();
      }
    }

    await finishRound();

    if (gameState) {
      io.to(GAME_ROOM).emit(
        "greedy_lion_clock",
        {
          roundNumber:
            gameState.roundNumber,
          roundEndsAt:
            gameState.roundEndsAt,
          serverTime: Date.now()
        }
      );
    }
  } catch (error) {
    console.error(
      "Game clock error:",
      error.message
    );
  }
}

let clockBusy = false;

setInterval(async () => {
  if (clockBusy) {
    return;
  }

  clockBusy = true;

  try {
    await refreshGame();
  } finally {
    clockBusy = false;
  }
}, 1000);

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
        audioBuffer,
        userId
      }
    );
  });

  // Greedy Lion events

  socket.on("greedy_lion_join", async () => {
    socket.join(GAME_ROOM);

    if (firebaseReady) {
      try {
        const snapshot = await database()
          .ref(GAME_PATH)
          .once("value");

        if (snapshot.exists()) {
          gameState = snapshot.val();
        }
      } catch (error) {
        console.error(
          "Join sync failed:",
          error.message
        );
      }
    }

    socket.emit(
      "greedy_lion_state",
      publicGameState()
    );
  });

  socket.on(
    "greedy_lion_place_bet",
    async (data, ack) => {
      const reply =
        typeof ack === "function"
          ? ack
          : () => {};

      if (!BETTING_ENABLED) {
        reply({
          ok: false,
          error: "BETTING_DISABLED",
          message:
            "الرهانات مغلقة مؤقتًا لحماية الرصيد"
        });
        return;
      }

      // Do not debit any wallet until the
      // authenticated settlement system
      // has passed integration testing.
      reply({
        ok: false,
        error: "BETTING_NOT_READY"
      });
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
    bettingEnabled: false
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
      "Startup error:",
      error.message
    );
  }
});
