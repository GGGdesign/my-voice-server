
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
const SECRET =
  "/etc/secrets/firebase-service-account.json";

const GAME_PATH = "greedyLionGame";
const GAME_ROOM = "maxchat_greedy_lion_global";
const ROUND_MS = 25000;

const MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];
const AMOUNTS = [100, 500, 1000, 5000, 10000];
const MAX_SLOTS = 6;
const MAX_BETS = 2000;

// Keep false until testing on a separate test database.
const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS === "true";

let firebaseReady = false;
let game = null;
let roundBusy = false;
let loading = false;

try {
  const credentials = JSON.parse(
    fs.readFileSync(SECRET, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
    databaseURL: DB_URL
  });

  firebaseReady = true;
  console.log("Firebase Admin ready");
} catch (error) {
  console.error("FIREBASE ERROR:", error.message);
}

function db() {
  return admin.database();
}

function gameRef() {
  return db().ref(GAME_PATH);
}

function validGame(value) {
  return Boolean(
    value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber > 0 &&
    Number.isFinite(Number(value.roundStartsAt)) &&
    Number.isFinite(Number(value.roundEndsAt))
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
        (a.roundNumber || 0) - (b.roundNumber || 0)
      )
      .slice(-8);
  }

  return [];
}

function coinsNumber(value) {
  if (
    typeof value === "string" &&
    /^\d+$/.test(value)
  ) {
    value = Number(value);
  }

  if (
    !Number.isSafeInteger(value) ||
    value < 0
  ) {
    return null;
  }

  return value;
}

function createGame(now = Date.now()) {
  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_MS,
    history: [],
    activeBets: {}
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

function state() {
  return {
    roundNumber: game ? game.roundNumber : null,
    roundStartsAt: game ? game.roundStartsAt : null,
    roundEndsAt: game ? game.roundEndsAt : null,
    serverTime: Date.now(),
    secondsRemaining: remaining(),
    bettingEnabled: BETTING_ENABLED,
    phase: BETTING_ENABLED
      ? "betting"
      : "betting_disabled",
    multipliers: MULTIPLIERS,
    history: game ? historyArray(game.history) : []
  };
}

function sendState() {
  if (game) {
    io.to(GAME_ROOM).emit(
      "greedy_lion_state",
      state()
    );
  }
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
      secondsRemaining: remaining()
    }
  );
}

// Load the game without locking the whole database.
async function loadGame() {
  if (!firebaseReady || loading) return;

  loading = true;

  try {
    const snapshot = await gameRef().once("value");
    let saved = snapshot.val();

    if (saved === null) {
      const now = Date.now();

      const tx = await gameRef().transaction(
        current => {
          if (current !== null) return;
          return createGame(now);
        },
        undefined,
        false
      );

      saved = tx.snapshot.val();
    }

    if (!validGame(saved)) {
      console.error("INVALID GAME DATA");
      return;
    }

    game = saved;
    console.log("GAME LOADED:", game.roundNumber);
    sendState();
  } catch (error) {
    console.error("LOAD ERROR:", error.message);
  } finally {
    loading = false;
  }
}

// Repair old 250-second rounds without deleting history.
async function repairRound() {
  if (!game || !firebaseReady || roundBusy) return;

  const start = Number(game.roundStartsAt);
  const end = Number(game.roundEndsAt);

  if (end - start === ROUND_MS) return;

  roundBusy = true;

  try {
    const now = Date.now();

    const tx = await gameRef().transaction(
      current => {
        if (!validGame(current)) return;

        const duration =
          Number(current.roundEndsAt) -
          Number(current.roundStartsAt);

        if (duration === ROUND_MS) return;

        // Do not rewrite the current round
        // if bets already exist.
        if (
          Object.keys(current.activeBets || {}).length
        ) {
          return;
        }

        return {
          ...current,
          roundStartsAt: now,
          roundEndsAt: now + ROUND_MS
        };
      },
      undefined,
      false
    );

    if (validGame(tx.snapshot.val())) {
      game = tx.snapshot.val();
    }

    if (tx.committed) {
      console.log("ROUND REPAIRED");
      sendState();
    }
  } catch (error) {
    console.error("REPAIR ERROR:", error.message);
  } finally {
    roundBusy = false;
  }
}

// The clock is independent from financial transactions.
async function finishRound() {
  if (
    !firebaseReady ||
    !game ||
    roundBusy ||
    Date.now() < Number(game.roundEndsAt)
  ) {
    return;
  }

  roundBusy = true;

  try {
    const now = Date.now();
    const oldRound = game.roundNumber;
    const winner = crypto.randomInt(8);

    // Game-only transaction for rounds without bets.
    // Financial settlement uses a separate path below.
    const hasBets =
      Object.keys(game.activeBets || {}).length > 0;

    if (hasBets && BETTING_ENABLED) {
      await settleRound(oldRound, winner, now);
      return;
    }

    const tx = await gameRef().transaction(
      current => {
        if (!validGame(current)) return;

        if (
          current.roundNumber !== oldRound ||
          now < Number(current.roundEndsAt)
        ) {
          return;
        }

        // Never erase unprocessed bets.
        if (
          Object.keys(current.activeBets || {}).length
        ) {
          return;
        }

        const result = {
          roundNumber: oldRound,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: false
        };

        return {
          ...current,
          roundNumber: oldRound + 1,
          roundStartsAt: now,
          roundEndsAt: now + ROUND_MS,
          activeBets: {},
          history: [
            ...historyArray(current.history),
            result
          ].slice(-8)
        };
      },
      undefined,
      false
    );

    if (validGame(tx.snapshot.val())) {
      game = tx.snapshot.val();
    }

    if (tx.committed) {
      console.log("ROUND FINISHED:", oldRound);
      sendResult(oldRound);
    }

    sendState();
  } catch (error) {
    console.error("ROUND ERROR:", error.message);
  } finally {
    roundBusy = false;
  }
}

function sendResult(oldRound) {
  const result = historyArray(game.history)
    .find(item => item.roundNumber === oldRound);

  if (result) {
    io.to(GAME_ROOM).emit(
      "greedy_lion_round_result",
      result
    );
  }
}

// Financial settlement is only called for rounds
// containing accepted bets.
async function settleRound(oldRound, winner, now) {
  try {
    const tx = await db().ref().transaction(
      root => {
        if (
          !root ||
          !validGame(root[GAME_PATH])
        ) {
          return;
        }

        const current = root[GAME_PATH];

        if (
          current.roundNumber !== oldRound ||
          now < Number(current.roundEndsAt)
        ) {
          return;
        }

        const bets = Object.values(
          current.activeBets || {}
        );

        if (bets.length > MAX_BETS) return;

        const winnings = {};

        for (const bet of bets) {
          if (
            !bet ||
            !bet.uid ||
            !Number.isInteger(bet.foodIndex) ||
            !AMOUNTS.includes(bet.amount)
          ) {
            return;
          }

          if (bet.foodIndex !== winner) continue;

          const payout =
            bet.amount * MULTIPLIERS[winner];

          const previous = winnings[bet.uid] || 0;
          const total = previous + payout;

          if (!Number.isSafeInteger(total)) {
            return;
          }

          winnings[bet.uid] = total;
        }

        for (const [uid, amount] of
          Object.entries(winnings)) {
          const user = root.Users?.[uid];
          const balance = user
            ? coinsNumber(user.coins)
            : null;

          if (
            balance === null ||
            !Number.isSafeInteger(balance + amount)
          ) {
            return;
          }
        }

        for (const [uid, amount] of
          Object.entries(winnings)) {
          const balance = coinsNumber(
            root.Users[uid].coins
          );

          root.Users[uid].coins = balance + amount;
        }

        const result = {
          roundNumber: oldRound,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets: true,
          winnings
        };

        root[GAME_PATH] = {
          ...current,
          roundNumber: oldRound + 1,
          roundStartsAt: now,
          roundEndsAt: now + ROUND_MS,
          activeBets: {},
          history: [
            ...historyArray(current.history),
            result
          ].slice(-8)
        };

        return root;
      },
      undefined,
      false
    );

    const latest = tx.snapshot.val()?.[GAME_PATH];

    if (validGame(latest)) {
      game = latest;
    }

    if (tx.committed) {
      console.log("ROUND SETTLED:", oldRound);
      sendResult(oldRound);
    }

    sendState();
  } catch (error) {
    console.error("SETTLEMENT ERROR:", error.message);
  }
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
      error: "GAME_UNAVAILABLE"
    };
  }

  if (!data || typeof data !== "object") {
    return {
      ok: false,
      error: "INVALID_REQUEST"
    };
  }

  const {
    idToken,
    betId,
    foodIndex,
    amount,
    roundNumber
  } = data;

  if (
    typeof idToken !== "string" ||
    typeof betId !== "string" ||
    !/^[a-fA-F0-9]{32}$/.test(betId) ||
    !Number.isInteger(foodIndex) ||
    foodIndex < 0 ||
    foodIndex > 7 ||
    !AMOUNTS.includes(amount) ||
    !Number.isSafeInteger(roundNumber)
  ) {
    return {
      ok: false,
      error: "INVALID_BET"
    };
  }

  let uid;

  try {
    const verified =
      await admin.auth().verifyIdToken(
        idToken,
        true
      );

    uid = verified.uid;
  } catch (error) {
    return {
      ok: false,
      error: "AUTH_FAILED"
    };
  }

  const now = Date.now();
  let reason = "BET_REJECTED";

  try {
    // One atomic debit + bet record.
    // This runs only when a player bets,
    // never once per clock tick.
    const tx = await db().ref().transaction(
      root => {
        if (
          !root ||
          !validGame(root[GAME_PATH])
        ) {
          reason = "GAME_UNAVAILABLE";
          return;
        }

        const current = root[GAME_PATH];

        if (
          current.roundNumber !== roundNumber ||
          now < Number(current.roundStartsAt) ||
          now >= Number(current.roundEndsAt) - 1000
        ) {
          reason = "ROUND_CLOSED";
          return;
        }

        const user = root.Users?.[uid];
        const balance = user
          ? coinsNumber(user.coins)
          : null;

        if (balance === null) {
          reason = "INVALID_WALLET";
          return;
        }

        const active = current.activeBets || {};
        const key = uid + "_" + betId;

        if (active[key]) {
          reason = "DUPLICATE_BET";
          return;
        }

        const allBets = Object.values(active);

        if (allBets.length >= MAX_BETS) {
          reason = "ROUND_FULL";
          return;
        }

        const playerBets = allBets.filter(
          bet => bet && bet.uid === uid
        );

        const slots = new Set(
          playerBets.map(bet => bet.foodIndex)
        );

        if (
          !slots.has(foodIndex) &&
          slots.size >= MAX_SLOTS
        ) {
          reason = "MAX_SLOTS";
          return;
        }

        if (balance < amount) {
          reason = "INSUFFICIENT_COINS";
          return;
        }

        user.coins = balance - amount;

        active[key] = {
          uid,
          betId,
          foodIndex,
          amount,
          roundNumber
        };

        current.activeBets = active;

        return root;
      },
      undefined,
      false
    );

    if (!tx.committed) {
      return {
        ok: false,
        error: reason
      };
    }

    // Refresh local game state after an accepted bet.
    const latest = tx.snapshot.val()?.[GAME_PATH];

    if (validGame(latest)) {
      game = latest;
    }

    return {
      ok: true,
      betId,
      amount,
      foodIndex,
      roundNumber
    };
  } catch (error) {
    console.error("BET ERROR:", error.message);

    return {
      ok: false,
      error: "SERVER_ERROR"
    };
  }
}

// One-second clock.
// Never wait for Firebase to emit clock events.
setInterval(() => {
  sendClock();

  if (!game) {
    loadGame().catch(console.error);
    return;
  }

  const duration =
    Number(game.roundEndsAt) -
    Number(game.roundStartsAt);

  if (duration !== ROUND_MS) {
    repairRound().catch(console.error);
    return;
  }

  if (Date.now() >= Number(game.roundEndsAt)) {
    finishRound().catch(console.error);
  }
}, 1000);

// Preserve Max Chat voice room events.
io.on("connection", socket => {
  console.log("User connected:", socket.id);

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
      { userId: data.userId }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(data.roomId).emit(
      "user_started_audio",
      { userId: data.userId }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(data.roomId).emit(
      "user_stopped_audio",
      { userId: data.userId }
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

    if (!game) {
      await loadGame();
    }

    if (game) {
      socket.emit(
        "greedy_lion_state",
        state()
      );
    }
  });

  socket.on("greedy_lion_place_bet",
    async (data, ack) => {
      const result = await placeBet(data);

      if (typeof ack === "function") {
        ack(result);
      }
    }
  );

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log("User disconnected:", socket.id);
  });
});

app.get("/", (req, res) => {
  const history = game
    ? historyArray(game.history)
    : [];

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled: BETTING_ENABLED,
    roundNumber: game?.roundNumber ?? null,
    secondsRemaining: remaining(),
    roundEndsAt: game?.roundEndsAt ?? null,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

server.listen(PORT, async () => {
  console.log("Max Chat server running:", PORT);

  await loadGame();

  console.log(
    "GAME READY:",
    game ? game.roundNumber : "NOT READY"
  );
});
