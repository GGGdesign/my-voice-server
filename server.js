
const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const { Server } = require("socket.io");
const admin = require("firebase-admin");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const DATABASE_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GAME_ROOM = "maxchat_greedy_lion_global";
const GAME_PATH = "greedyLionGame";
const ROUND_MS = 25000;
const MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];

// Keep wallet transactions disabled.
const BETTING_ENABLED = false;

let firebaseReady = false;
let game = null;
let roundBusy = false;
let loading = false;

try {
  if (fs.existsSync(SECRET_PATH)) {
    const key = JSON.parse(
      fs.readFileSync(SECRET_PATH, "utf8")
    );

    admin.initializeApp({
      credential: admin.credential.cert(key),
      databaseURL: DATABASE_URL
    });

    firebaseReady = true;
    console.log("Firebase Admin ready");
  } else {
    console.error("Firebase secret missing");
  }
} catch (error) {
  console.error("Firebase error:", error.message);
}

function ref() {
  return admin.database().ref(GAME_PATH);
}

function historyArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).slice(-8);
  }

  if (value && typeof value === "object") {
    return Object.values(value)
      .filter(item => item && typeof item === "object")
      .sort((a, b) =>
        (a.roundNumber || 0) - (b.roundNumber || 0)
      )
      .slice(-8);
  }

  return [];
}

function validGame(value) {
  return value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber >= 1 &&
    Number.isFinite(Number(value.roundStartsAt)) &&
    Number.isFinite(Number(value.roundEndsAt));
}

function freshGame() {
  const now = Date.now();

  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: []
  };
}

function normalizeGame(value) {
  return {
    roundNumber: value.roundNumber,
    roundStartsAt: Number(value.roundStartsAt),
    roundEndsAt: Number(value.roundEndsAt),
    history: historyArray(value.history)
  };
}

function secondsRemaining() {
  if (!game) return null;

  return Math.max(
    0,
    Math.ceil((game.roundEndsAt - Date.now()) / 1000)
  );
}

function publicState() {
  const now = Date.now();

  return {
    roundNumber: game ? game.roundNumber : null,
    roundStartsAt: game ? game.roundStartsAt : null,
    roundEndsAt: game ? game.roundEndsAt : null,
    serverTime: now,
    secondsRemaining: secondsRemaining(),
    phase: "betting_disabled",
    bettingEnabled: BETTING_ENABLED,
    history: game ? game.history : [],
    multipliers: MULTIPLIERS
  };
}

function sendState() {
  if (!game) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_state",
    publicState()
  );
}

function sendClock() {
  if (!game) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_clock",
    {
      roundNumber: game.roundNumber,
      roundStartsAt: game.roundStartsAt,
      roundEndsAt: game.roundEndsAt,
      serverTime: Date.now(),
      secondsRemaining: secondsRemaining()
    }
  );
}

async function loadGame() {
  if (!firebaseReady || loading) return;

  loading = true;

  try {
    const snapshot = await ref().once("value");
    const saved = snapshot.val();

    if (saved === null) {
      const tx = await ref().transaction(
        current => current === null
          ? freshGame()
          : undefined,
        undefined,
        false
      );

      const value = tx.snapshot.val();

      if (validGame(value)) {
        game = normalizeGame(value);
      }
    } else if (validGame(saved)) {
      game = normalizeGame(saved);
    } else {
      console.error(
        "Invalid saved game. Existing data preserved."
      );
    }

    if (game) {
      console.log("GAME LOADED:", game.roundNumber);
      sendState();
    }
  } catch (error) {
    console.error("LOAD ERROR:", error.message);
  } finally {
    loading = false;
  }
}

async function finishRound() {
  if (!firebaseReady || roundBusy || !game) return;
  if (Date.now() < game.roundEndsAt) return;

  roundBusy = true;

  try {
    const oldRound = game.roundNumber;
    const winner = crypto.randomInt(8);

    const tx = await ref().transaction(
      current => {
        if (!validGame(current)) return;

        const currentGame = normalizeGame(current);

        if (Date.now() < currentGame.roundEndsAt) {
          return;
        }

        if (currentGame.roundNumber !== oldRound) {
          return;
        }

        const now = Date.now();

        const result = {
          roundNumber: currentGame.roundNumber,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        return {
          roundNumber: currentGame.roundNumber + 1,
          roundStartsAt: now,
          roundEndsAt: now + ROUND_MS,
          history: [
            ...currentGame.history,
            result
          ].slice(-8)
        };
      },
      undefined,
      false
    );

    const saved = tx.snapshot.val();

    if (validGame(saved)) {
      game = normalizeGame(saved);
    }

    if (tx.committed && game) {
      const result = game.history.find(
        item => item.roundNumber === oldRound
      );

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );

        console.log(
          "ROUND FINISHED:",
          oldRound,
          "WINNER:",
          winner
        );
      }

      console.log("ROUND STARTED:", game.roundNumber);
    }

    sendState();
    sendClock();

  } catch (error) {
    console.error("ROUND ERROR:", error.message);

    // Retry on the next clock tick.
    try {
      const snapshot = await ref().once("value");
      if (validGame(snapshot.val())) {
        game = normalizeGame(snapshot.val());
      }
    } catch (readError) {
      console.error("SYNC ERROR:", readError.message);
    }
  } finally {
    roundBusy = false;
  }
}

// Keep sending clock updates independently
// from Firebase round transactions.
setInterval(() => {
  sendClock();

  if (!game) {
    loadGame().catch(console.error);
    return;
  }

  if (Date.now() >= game.roundEndsAt) {
    finishRound().catch(console.error);
  }
}, 1000);

// Existing voice room events
io.on("connection", socket => {
  console.log("User connected:", socket.id);

  socket.on("join_room", data => {
    if (!data || !data.roomId) return;
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
    if (!data || !data.roomId) return;

    io.to(data.roomId).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data || !data.roomId) return;

    io.to(data.roomId).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data || !data.roomId) return;

    io.to(data.roomId).emit(
      "user_left_seat",
      { userId: data.userId }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "user_started_audio",
      { userId: data.userId }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "user_stopped_audio",
      { userId: data.userId }
    );
  });

  socket.on("voice_data", data => {
    if (!data || !data.roomId) return;

    socket.to(data.roomId).emit(
      "voice_data",
      {
        audioBuffer: data.audioBuffer,
        userId: data.userId
      }
    );
  });

  socket.on("greedy_lion_join", () => {
    socket.join(GAME_ROOM);

    if (game) {
      socket.emit("greedy_lion_state", publicState());
      socket.emit(
        "greedy_lion_clock",
        {
          roundNumber: game.roundNumber,
          roundStartsAt: game.roundStartsAt,
          roundEndsAt: game.roundEndsAt,
          serverTime: Date.now(),
          secondsRemaining: secondsRemaining()
        }
      );
    } else {
      loadGame().then(() => {
        if (game && socket.connected) {
          socket.emit(
            "greedy_lion_state",
            publicState()
          );
        }
      }).catch(console.error);
    }

    console.log("Greedy Lion player joined");
  });

  socket.on("greedy_lion_place_bet", (data, ack) => {
    if (typeof ack === "function") {
      ack({
        ok: false,
        error: "BETTING_DISABLED",
        message: "الرهانات مغلقة لحماية الرصيد"
      });
    }
  });

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
  });
});

app.get("/", (req, res) => {
  const history = game ? game.history : [];

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: false,
    roundNumber: game ? game.roundNumber : null,
    secondsRemaining: secondsRemaining(),
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, async () => {
  console.log("Max Chat server running on port", PORT);

  await loadGame();

  if (game) {
    console.log("GAME READY:", game.roundNumber);

    if (Date.now() >= game.roundEndsAt) {
      await finishRound();
    }
  }
});
