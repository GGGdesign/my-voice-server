
"use strict";

const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 10 * 1024 * 1024
});

app.use(express.json({ limit: "32kb" }));

const PORT = Number(process.env.PORT || 10000);

const DB_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GAME_PATH = "greedyLionGame";
const GAME_ROOM = "maxchat_greedy_lion_global";

const BET_MS = 25000;
const RESULT_MS = 6000;
const CYCLE_MS = BET_MS + RESULT_MS;

const MULTIPLIERS = [
  45, 5, 5, 10, 15, 5, 5, 25
];

const CHIPS = [
  100, 500, 1000, 5000, 10000
];

const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS !== "false";

const GIFTS_ENABLED =
  process.env.MAXCHAT_ENABLE_GIFTS !== "false";

const GIFTS = Object.freeze({
  rose: { name: "وردة ملكية", price: 100 },
  heart: { name: "قلب الحب", price: 200 },
  bear: { name: "دبدوب", price: 500 },
  crown: { name: "تاج ملكي", price: 1000 },
  bouquet: { name: "باقة ورد", price: 2000 },
  car: { name: "سيارة فاخرة", price: 5000 },
  yacht: { name: "يخت", price: 10000 },
  palace: { name: "قصر", price: 20000 },
  diamond: { name: "ألماس", price: 50000 },
  horse: { name: "حصان ملكي", price: 100000 },
  dragon: { name: "تنين أسطوري", price: 500000 },
  galaxy: { name: "المجرة", price: 1000000 },
  tree: { name: "شجرة الذهب", price: 2000000 },
  throne: { name: "عرش ملكي", price: 5000000 },
  kingdom: { name: "مملكة الذهب", price: 10000000 }
});

let firebaseReady = false;

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

function db() {
  if (!firebaseReady) {
    throw new Error("FIREBASE_NOT_READY");
  }
  return admin.database();
}

function gameRef() {
  return db().ref(GAME_PATH);
}

function userRef(uid) {
  return db().ref("Users").child(uid);
}

function validId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{1,100}$/.test(value);
}

function validRequestId(value) {
  return typeof value === "string" &&
    /^[a-zA-Z0-9_-]{16,100}$/.test(value);
}

function validCoins(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function errorResult(error) {
  return { ok: false, error };
}

async function transactionReady(ref, updater) {
  let listener = null;

  try {
    await new Promise((resolve, reject) => {
      listener = snapshot => resolve(snapshot);
      ref.on("value", listener, reject);
    });

    return await ref.transaction(
      updater,
      undefined,
      false
    );
  } finally {
    if (listener) {
      ref.off("value", listener);
    }
  }
}

async function verifyToken(token) {
  if (!firebaseReady) {
    throw new Error("SERVER_NOT_READY");
  }

  if (!token || typeof token !== "string") {
    throw new Error("TOKEN_REQUIRED");
  }

  try {
    return await admin.auth().verifyIdToken(
      token,
      true
    );
  } catch {
    throw new Error("INVALID_TOKEN");
  }
}

async function verifyHttpUser(req) {
  const header = String(
    req.headers.authorization || ""
  );

  const match = /^Bearer\s+(.+)$/i.exec(header);

  if (!match) {
    throw new Error("TOKEN_REQUIRED");
  }

  return verifyToken(match[1]);
}

async function readWallet(uid, operation) {
  if (!validId(uid)) {
    return errorResult("INVALID_UID");
  }

  const snapshot = await userRef(uid).once("value");

  if (!snapshot.exists()) {
    console.error("USER NOT FOUND", operation, uid);
    return errorResult("USER_NOT_FOUND");
  }

  const user = snapshot.val();

  if (!user || typeof user !== "object") {
    return errorResult("INVALID_USER");
  }

  const coins = Number(user.coins);

  if (!validCoins(coins)) {
    return errorResult("INVALID_WALLET");
  }

  return {
    ok: true,
    uid,
    coins,
    user
  };
}

// ===============================================
// GAME STATE
// ===============================================

let game = null;
let gameStarted = false;
let roundProcessing = false;
let settlementProcessing = false;

function historyArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean).slice(-8);
  }

  if (value && typeof value === "object") {
    return Object.values(value)
      .filter(Boolean)
      .sort(
        (a, b) =>
          Number(a.roundNumber || 0) -
          Number(b.roundNumber || 0)
      )
      .slice(-8);
  }

  return [];
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

function cycleEnd(value) {
  return Number(
    value.cycleEndsAt ||
    (Number(value.roundEndsAt) + RESULT_MS)
  );
}

function gamePhase(value, now = Date.now()) {
  if (!validGame(value)) {
    return "waiting";
  }

  if (now < Number(value.roundEndsAt)) {
    return "betting";
  }

  return "result";
}

function createGame(now) {
  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + BET_MS,
    cycleEndsAt: now + CYCLE_MS,
    activeBets: {},
    pendingSettlement: {},
    history: []
  };
}

function secondsUntil(time) {
  return Math.max(
    0,
    Math.ceil((Number(time) - Date.now()) / 1000)
  );
}

function remaining() {
  if (!game) {
    return null;
  }

  return secondsUntil(
    gamePhase(game) === "betting"
      ? game.roundEndsAt
      : cycleEnd(game)
  );
}

function publicState() {
  const phase = gamePhase(game);

  return {
    roundNumber: game?.roundNumber ?? null,
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    cycleEndsAt: game ? cycleEnd(game) : null,
    serverTime: Date.now(),
    secondsRemaining: remaining(),
    bettingEnabled:
      firebaseReady &&
      BETTING_ENABLED &&
      phase === "betting",
    phase,
    multipliers: MULTIPLIERS,
    history: historyArray(game?.history)
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

  const state = publicState();

  io.to(GAME_ROOM).emit(
    "greedy_lion_clock",
    {
      roundNumber: state.roundNumber,
      roundStartsAt: state.roundStartsAt,
      roundEndsAt: state.roundEndsAt,
      cycleEndsAt: state.cycleEndsAt,
      serverTime: state.serverTime,
      secondsRemaining: state.secondsRemaining,
      bettingEnabled: state.bettingEnabled,
      phase: state.phase
    }
  );
}

// ===============================================
// PLAYER PROFILES
// ===============================================

function profileName(user) {
  return String(
    user?.name ||
    user?.userName ||
    user?.username ||
    user?.nickname ||
    "مستخدم"
  );
}

function profilePhoto(user) {
  return String(
    user?.profileImageUrl ||
    user?.profileImage ||
    user?.photoUrl ||
    user?.photoURL ||
    user?.avatarUrl ||
    user?.avatar ||
    user?.imageUrl ||
    ""
  );
}

function calculatePayouts(entry) {
  const totals = {};
  const winner = Number(entry.winningIndex);

  for (const bet of Object.values(entry.bets || {})) {
    const uid = String(bet.uid || "");
    const index = Number(bet.foodIndex);
    const amount = Number(bet.amount);

    if (
      !validId(uid) ||
      index !== winner ||
      !validCoins(amount) ||
      amount === 0
    ) {
      continue;
    }

    const payout = amount * MULTIPLIERS[index];

    if (!validCoins(payout)) {
      continue;
    }

    const next = (totals[uid] || 0) + payout;

    if (!validCoins(next)) {
      continue;
    }

    totals[uid] = next;
  }

  return totals;
}

async function buildWinnerDetails(entry) {
  const totals = calculatePayouts(entry);

  const ordered = Object.entries(totals)
    .sort((a, b) => b[1] - a[1]);

  return Promise.all(
    ordered.map(async ([uid, amount], index) => {
      let user = {};

      try {
        const snapshot = await userRef(uid).once("value");
        user = snapshot.val() || {};
      } catch (error) {
        console.error(
          "WINNER PROFILE ERROR",
          uid,
          error.message
        );
      }

      const name = profileName(user);
      const photoUrl = profilePhoto(user);

      return {
        rank: index + 1,
        uid,
        userUid: uid,
        userId: String(
          user.customId ||
          user.userId ||
          ""
        ),
        name,
        userName: name,
        photoUrl,
        avatarUrl: photoUrl,
        amount,
        payout: amount,
        winnings: amount,
        winningIndex: Number(entry.winningIndex)
      };
    })
  );
}

async function publishRoundWinners(entry) {
  const roundNumber = Number(entry.roundNumber);
  const winners = await buildWinnerDetails(entry);

  const result = {
    roundNumber,
    winningIndex: Number(entry.winningIndex),
    completedAt: Number(entry.completedAt),
    hasSettledBets: true,
    winners,
    topWinners: winners.slice(0, 3)
  };

  const tx = await transactionReady(
    gameRef(),
    current => {
      if (!validGame(current)) return;

      const history = historyArray(current.history);

      const index = history.findIndex(
        item =>
          Number(item.roundNumber) === roundNumber
      );

      if (index < 0) return;

      history[index] = {
        ...history[index],
        ...result
      };

      return {
        ...current,
        history
      };
    }
  );

  const saved = historyArray(
    tx.snapshot.val()?.history
  ).find(
    item =>
      Number(item.roundNumber) === roundNumber
  );

  if (!saved || !saved.hasSettledBets) {
    throw new Error("WINNER_HISTORY_NOT_SAVED");
  }

  io.to(GAME_ROOM).emit(
    "greedy_lion_winners",
    result
  );

  io.to(GAME_ROOM).emit(
    "greedy_lion_round_result",
    result
  );

  return result;
}

// ===============================================
// START GAME
// ===============================================

async function startGame() {
  if (!firebaseReady || gameStarted) {
    return;
  }

  gameStarted = true;

  try {
    const ref = gameRef();
    const snapshot = await ref.once("value");

    if (!snapshot.exists()) {
      await transactionReady(ref, current => {
        if (current !== null) return;
        return createGame(Date.now());
      });
    }

    ref.on(
      "value",
      snapshot => {
        const saved = snapshot.val();

        if (!validGame(saved)) {
          console.error("INVALID GAME STATE");
          return;
        }

        game = saved;
        sendState();
      },
      error => {
        console.error(
          "GAME LISTENER ERROR",
          error.message
        );
      }
    );

    console.log("GAME LISTENER STARTED");
  } catch (error) {
    gameStarted = false;
    console.error(
      "START GAME ERROR",
      error.message
    );
  }
}

// ===============================================
// CLOSE ROUND / SHOW RESULT
// ===============================================

async function closeRound() {
  if (
    !firebaseReady ||
    !game ||
    roundProcessing
  ) {
    return;
  }

  roundProcessing = true;

  try {
    const winner = crypto.randomInt(8);
    const now = Date.now();

    const tx = await transactionReady(
      gameRef(),
      current => {
        if (!validGame(current)) return;

        if (Date.now() < Number(current.roundEndsAt)) {
          return;
        }

        const roundNumber = Number(
          current.roundNumber
        );

        const history = historyArray(
          current.history
        );

        if (
          history.some(
            item =>
              Number(item.roundNumber) === roundNumber
          )
        ) {
          return;
        }

        const bets = current.activeBets || {};

        const pending = {
          ...(current.pendingSettlement || {})
        };

        const result = {
          roundNumber,
          winningIndex: winner,
          completedAt: now,
          hasSettledBets:
            Object.keys(bets).length === 0,
          winners: [],
          topWinners: []
        };

        if (Object.keys(bets).length > 0) {
          pending[String(roundNumber)] = {
            roundNumber,
            winningIndex: winner,
            completedAt: now,
            payoutVersion: 2,
            settled: false,
            bets
          };
        }

        return {
          ...current,
          pendingSettlement: pending,
          history: [
            ...history,
            result
          ].slice(-8)
        };
      }
    );

    if (!tx.committed) return;

    game = tx.snapshot.val();

    const result = historyArray(game.history)
      .find(
        item =>
          Number(item.roundNumber) ===
          Number(game.roundNumber)
      );

    if (result) {
      io.to(GAME_ROOM).emit(
        "greedy_lion_round_result",
        result
      );
    }

    sendState();
    sendClock();

    setImmediate(() => {
      settlePending().catch(error => {
        console.error(
          "IMMEDIATE SETTLEMENT ERROR",
          error.message
        );
      });
    });
  } catch (error) {
    console.error(
      "CLOSE ROUND ERROR",
      error.message
    );
  } finally {
    roundProcessing = false;
  }
}

// ===============================================
// START NEXT ROUND
// ===============================================

async function nextRound() {
  if (
    !firebaseReady ||
    !game ||
    roundProcessing
  ) {
    return;
  }

  roundProcessing = true;

  try {
    const now = Date.now();

    const tx = await transactionReady(
      gameRef(),
      current => {
        if (!validGame(current)) return;

        if (Date.now() < cycleEnd(current)) {
          return;
        }

        const currentRound = Number(
          current.roundNumber
        );

        const history = historyArray(
          current.history
        );

        // Never discard bets from a round that
        // has not been closed and recorded.
        const recorded = history.some(
          item =>
            Number(item.roundNumber) === currentRound
        );

        if (!recorded) {
          return;
        }

        return {
          ...current,
          roundNumber: currentRound + 1,
          roundStartsAt: now,
          roundEndsAt: now + BET_MS,
          cycleEndsAt: now + CYCLE_MS,
          activeBets: {}
        };
      }
    );

    if (!tx.committed) return;

    game = tx.snapshot.val();

    sendState();
    sendClock();
  } catch (error) {
    console.error(
      "NEXT ROUND ERROR",
      error.message
    );
  } finally {
    roundProcessing = false;
  }
}

// ===============================================
// WALLET / BETS
// ===============================================

async function reserveCoins(
  uid,
  betId,
  amount,
  roundNumber
) {
  const wallet = await readWallet(
    uid,
    "BET_RESERVE"
  );

  if (!wallet.ok) return wallet;

  let reason = "WALLET_REJECTED";

  const tx = await transactionReady(
    userRef(uid),
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

      if (!validCoins(coins)) {
        reason = "INVALID_WALLET";
        return;
      }

      if (coins < amount) {
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
    }
  );

  if (!tx.committed) {
    return errorResult(reason);
  }

  return {
    ok: true,
    balance: Number(
      tx.snapshot.val()?.coins
    )
  };
}

async function setBetStatus(uid, betId, status) {
  const tx = await transactionReady(
    userRef(uid),
    user => {
      const bet = user?.lionBets?.[betId];

      if (!bet) return;

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
    }
  );

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]
      ?.status === status
  );
}

async function refundBet(uid, betId) {
  const tx = await transactionReady(
    userRef(uid),
    user => {
      const bet = user?.lionBets?.[betId];

      if (!bet) return;
      if (bet.status === "refunded") return;
      if (bet.status !== "reserved") return;

      const amount = Number(bet.amount);
      const coins = Number(user.coins);

      if (
        !validCoins(amount) ||
        !validCoins(coins) ||
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
    }
  );

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]
      ?.status === "refunded"
  );
}

async function placeBet(data) {
  if (!BETTING_ENABLED) {
    return errorResult("BETTING_DISABLED");
  }

  if (!firebaseReady || !game) {
    return errorResult("SERVER_NOT_READY");
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
    return errorResult("INVALID_BET");
  }

  let decoded;

  try {
    decoded = await verifyToken(token);
  } catch (error) {
    return errorResult(error.message);
  }

  const uid = decoded.uid;
  const betKey = `${uid}_${betId}`;

  const wallet = await readWallet(
    uid,
    "BET_REQUEST"
  );

  if (!wallet.ok) return wallet;

  const currentGame = (
    await gameRef().once("value")
  ).val();

  if (
    !validGame(currentGame) ||
    Number(currentGame.roundNumber) !== roundNumber ||
    gamePhase(currentGame) !== "betting"
  ) {
    return errorResult("ROUND_CLOSED");
  }

  // Pre-check the six-slot rule before
  // temporarily reserving any coins.
  const existing = Object.values(
    currentGame.activeBets || {}
  );

  const selected = new Set(
    existing
      .filter(bet => bet.uid === uid)
      .map(bet => Number(bet.foodIndex))
  );

  if (
    !selected.has(foodIndex) &&
    selected.size >= 6
  ) {
    return errorResult("MAX_SIX_SLOTS");
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
    console.error(
      "BET WALLET ERROR",
      error.message
    );
    return errorResult("WALLET_ERROR");
  }

  if (!reserved.ok) return reserved;

  let reason = "BET_REJECTED";

  try {
    const tx = await transactionReady(
      gameRef(),
      current => {
        if (!validGame(current)) {
          reason = "GAME_NOT_READY";
          return;
        }

        if (
          Number(current.roundNumber) !== roundNumber ||
          gamePhase(current) !== "betting"
        ) {
          reason = "ROUND_CLOSED";
          return;
        }

        const activeBets = {
          ...(current.activeBets || {})
        };

        if (activeBets[betKey]) {
          reason = "DUPLICATE_BET";
          return;
        }

        const selectedSlots = new Set();

        for (const bet of Object.values(activeBets)) {
          if (bet.uid === uid) {
            selectedSlots.add(
              Number(bet.foodIndex)
            );
          }
        }

        if (
          !selectedSlots.has(foodIndex) &&
          selectedSlots.size >= 6
        ) {
          reason = "MAX_SIX_SLOTS";
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
      }
    );

    if (!tx.committed) {
      const refunded = await refundBet(
        uid,
        betId
      );

      return errorResult(
        refunded
          ? reason
          : "REFUND_PENDING"
      );
    }

    game = tx.snapshot.val();

    try {
      await setBetStatus(
        uid,
        betId,
        "accepted"
      );
    } catch (error) {
      console.error(
        "BET STATUS ERROR",
        error.message
      );
    }

    return {
      ok: true,
      betId,
      foodIndex,
      amount,
      roundNumber,
      balance: reserved.balance
    };
  } catch (error) {
    console.error(
      "BET TRANSACTION ERROR",
      error.message
    );

    try {
      const saved = (
        await gameRef().once("value")
      ).val();

      const inActive = Boolean(
        saved?.activeBets?.[betKey]
      );

      const inPending = Object.values(
        saved?.pendingSettlement || {}
      ).some(
        entry =>
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

      const refunded = await refundBet(
        uid,
        betId
      );

      return errorResult(
        refunded
          ? "BET_FAILED_REFUNDED"
          : "BET_STATUS_UNCERTAIN"
      );
    } catch {
      return errorResult(
        "BET_STATUS_UNCERTAIN"
      );
    }
  }
}

// ===============================================
// PAY WINNERS ONCE
// ===============================================

async function creditWinner(
  uid,
  roundNumber,
  amount
) {
  const key = String(roundNumber);

  const wallet = await readWallet(
    uid,
    "LION_PAYOUT"
  );

  if (!wallet.ok) return false;

  const tx = await transactionReady(
    userRef(uid),
    user => {
      if (!user) return;

      const payouts = user.lionPayouts || {};

      if (payouts[key]) return;

      const coins = Number(user.coins);

      if (
        !validCoins(coins) ||
        !validCoins(amount) ||
        amount === 0 ||
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
    }
  );

  return Boolean(
    tx.committed ||
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
      if (!entry) continue;

      if (
        entry.payoutVersion !== 1 &&
        entry.payoutVersion !== 2
      ) {
        continue;
      }

      if (!entry.settled) {
        const totals = calculatePayouts(entry);
        let allPaid = true;

        for (const [uid, amount] of Object.entries(totals)) {
          try {
            const paid = await creditWinner(
              uid,
              entry.roundNumber,
              amount
            );

            if (!paid) {
              allPaid = false;
            }
          } catch (error) {
            allPaid = false;

            console.error(
              "PAYOUT ERROR",
              uid,
              error.message
            );
          }
        }

        if (!allPaid) {
          continue;
        }

        await gameRef()
          .child("pendingSettlement")
          .child(key)
          .update({
            settled: true,
            settledAt: Date.now()
          });
      }

      // Retry publishing winners if payment
      // completed but result publication failed.
      const latestHistory = (
        await gameRef()
          .child("history")
          .once("value")
      ).val();

      const published = historyArray(
        latestHistory
      ).some(
        item =>
          Number(item.roundNumber) ===
            Number(entry.roundNumber) &&
          item.hasSettledBets === true
      );

      if (!published) {
        try {
          await publishRoundWinners(entry);
        } catch (error) {
          console.error(
            "WINNER DISPLAY ERROR",
            key,
            error.message
          );
        }
      }
    }
  } catch (error) {
    console.error(
      "SETTLEMENT ERROR",
      error.message
    );
  } finally {
    settlementProcessing = false;
  }
}

// ===============================================
// GIFTS
// ===============================================

function sameGiftPayment(payment, request) {
  return Boolean(
    payment &&
    payment.roomId === request.roomId &&
    payment.giftId === request.giftId &&
    payment.receiverId === request.receiverId &&
    payment.quantity === request.quantity &&
    payment.amount === request.amount
  );
}

async function giftReceiverOnSeat(
  roomId,
  receiverId
) {
  const snapshot = await db()
    .ref("rooms")
    .child(roomId)
    .child("seats")
    .once("value");

  let found = false;

  snapshot.forEach(seat => {
    const userId = String(
      seat.child("userId").val() || ""
    );

    const userUid = String(
      seat.child("userUid").val() || ""
    );

    if (
      userId === receiverId ||
      userUid === receiverId
    ) {
      found = true;
    }
  });

  return found;
}

async function chargeGift(uid, request) {
  const wallet = await readWallet(
    uid,
    "GIFT_SEND"
  );

  if (!wallet.ok) return wallet;

  let reason = "PAYMENT_REJECTED";

  const tx = await transactionReady(
    userRef(uid),
    user => {
      if (!user) {
        reason = "USER_NOT_FOUND";
        return;
      }

      const payments = user.giftPayments || {};
      const previous = payments[request.requestId];

      if (previous) {
        reason = sameGiftPayment(
          previous,
          request
        )
          ? "ALREADY_PAID"
          : "REQUEST_ID_CONFLICT";
        return;
      }

      const coins = Number(user.coins);

      if (!validCoins(coins)) {
        reason = "INVALID_WALLET";
        return;
      }

      if (coins < request.amount) {
        reason = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...user,
        coins: coins - request.amount,
        giftPayments: {
          ...payments,
          [request.requestId]: {
            roomId: request.roomId,
            giftId: request.giftId,
            receiverId: request.receiverId,
            quantity: request.quantity,
            amount: request.amount,
            status: "paid",
            createdAt: Date.now()
          }
        }
      };
    }
  );

  const saved = tx.snapshot.val();

  const payment =
    saved?.giftPayments?.[request.requestId];

  if (!sameGiftPayment(payment, request)) {
    return errorResult(reason);
  }

  return {
    ok: true,
    newlyCharged: tx.committed,
    balance: Number(saved.coins),
    senderId: String(saved.customId || ""),
    senderName: profileName(saved),
    createdAt: payment.createdAt
  };
}

async function publishGift(
  uid,
  request,
  payment
) {
  const eventId =
    `${uid}_${request.requestId}`;

  const event = {
    requestId: request.requestId,
    senderUid: uid,
    senderId: payment.senderId,
    senderName: payment.senderName,
    receiverId: request.receiverId,
    giftId: request.giftId,
    quantity: request.quantity,
    amount: request.amount,
    createdAt: payment.createdAt
  };

  const ref = db()
    .ref("rooms")
    .child(request.roomId)
    .child("giftEvents")
    .child(eventId);

  await transactionReady(ref, current => {
    if (current !== null) return;
    return event;
  });

  const saved = (
    await ref.once("value")
  ).val();

  if (
    !saved ||
    saved.senderUid !== uid ||
    saved.requestId !== request.requestId ||
    saved.giftId !== request.giftId ||
    saved.receiverId !== request.receiverId ||
    saved.quantity !== request.quantity ||
    saved.amount !== request.amount
  ) {
    throw new Error("GIFT_EVENT_CONFLICT");
  }

  return saved;
}

function giftError(res, status, error) {
  return res.status(status).json({
    ok: false,
    error
  });
}

app.get("/api/gifts/catalog", (req, res) => {
  res.json({
    ok: true,
    enabled: GIFTS_ENABLED && firebaseReady,
    gifts: GIFTS
  });
});

app.post("/api/gifts/send", async (req, res) => {
  if (!firebaseReady) {
    return giftError(
      res,
      503,
      "SERVER_NOT_READY"
    );
  }

  if (!GIFTS_ENABLED) {
    return giftError(
      res,
      503,
      "GIFTS_DISABLED"
    );
  }

  let decoded;

  try {
    decoded = await verifyHttpUser(req);
  } catch (error) {
    return giftError(
      res,
      401,
      error.message
    );
  }

  const uid = decoded.uid;

  const roomId = String(
    req.body?.roomId || ""
  );

  const giftId = String(
    req.body?.giftId || ""
  );

  const receiverId = String(
    req.body?.receiverId || ""
  );

  const requestId = String(
    req.body?.requestId || ""
  );

  const quantity = req.body?.quantity;

  if (
    !validId(roomId) ||
    !validId(giftId) ||
    !validId(receiverId) ||
    !validRequestId(requestId) ||
    !Number.isSafeInteger(quantity) ||
    quantity < 1 ||
    quantity > 99
  ) {
    return giftError(
      res,
      400,
      "INVALID_GIFT_REQUEST"
    );
  }

  const gift = GIFTS[giftId];

  if (!gift) {
    return giftError(
      res,
      400,
      "UNKNOWN_GIFT"
    );
  }

  const amount = gift.price * quantity;

  if (
    !validCoins(amount) ||
    amount === 0
  ) {
    return giftError(
      res,
      400,
      "INVALID_AMOUNT"
    );
  }

  const request = {
    roomId,
    giftId,
    receiverId,
    requestId,
    quantity,
    amount
  };

  try {
    const wallet = await readWallet(
      uid,
      "GIFT_REQUEST"
    );

    if (!wallet.ok) {
      return giftError(
        res,
        409,
        wallet.error
      );
    }

    const previous =
      wallet.user.giftPayments?.[requestId];

    if (previous) {
      if (
        !sameGiftPayment(
          previous,
          request
        )
      ) {
        return giftError(
          res,
          409,
          "REQUEST_ID_CONFLICT"
        );
      }
    } else {
      const exists = await giftReceiverOnSeat(
        roomId,
        receiverId
      );

      if (!exists) {
        return giftError(
          res,
          404,
          "RECEIVER_NOT_ON_SEAT"
        );
      }
    }

    const payment = await chargeGift(
      uid,
      request
    );

    if (!payment.ok) {
      return giftError(
        res,
        payment.error === "INSUFFICIENT_COINS"
          ? 402
          : 409,
        payment.error
      );
    }

    await publishGift(
      uid,
      request,
      payment
    );

    return res.json({
      ok: true,
      requestId,
      roomId,
      giftId,
      receiverId,
      quantity,
      amount,
      balance: payment.balance,
      alreadyPaid: !payment.newlyCharged
    });
  } catch (error) {
    console.error(
      "GIFT SEND ERROR",
      error.message
    );

    return res.status(503).json({
      ok: false,
      error: "GIFT_STATUS_UNCERTAIN",
      requestId
    });
  }
});

// ===============================================
// SOCKET / VOICE
// ===============================================

io.on("connection", socket => {
  console.log(
    "USER CONNECTED",
    socket.id
  );

  socket.on("join_room", data => {
    if (!data?.roomId) return;

    const roomId = String(data.roomId);

    socket.join(roomId);

    socket.to(roomId).emit(
      "user_joined_room",
      {
        userId: data.userId,
        userName: data.userName
      }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data?.roomId) return;

    io.to(String(data.roomId)).emit(
      "user_left_seat",
      {
        userId: data.userId
      }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
      "user_started_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
      "user_stopped_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("voice_data", data => {
    if (!data?.roomId) return;

    socket.to(String(data.roomId)).emit(
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
      await startGame();
    }

    if (!socket.connected || !game) {
      return;
    }

    socket.emit(
      "greedy_lion_state",
      publicState()
    );

    const state = publicState();

    socket.emit(
      "greedy_lion_clock",
      {
        roundNumber: state.roundNumber,
        roundStartsAt: state.roundStartsAt,
        roundEndsAt: state.roundEndsAt,
        cycleEndsAt: state.cycleEndsAt,
        serverTime: state.serverTime,
        secondsRemaining: state.secondsRemaining,
        bettingEnabled: state.bettingEnabled,
        phase: state.phase
      }
    );
  });

  socket.on(
    "greedy_lion_place_bet",
    async (data, ack) => {
      const reply =
        typeof ack === "function"
          ? ack
          : () => {};

      try {
        const result = await placeBet(data);
        reply(result);
      } catch (error) {
        console.error(
          "SOCKET BET ERROR",
          error.message
        );

        reply(
          errorResult("SERVER_ERROR")
        );
      }
    }
  );

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log(
      "USER DISCONNECTED",
      socket.id
    );
  });
});

// ===============================================
// TIMERS
// ===============================================

let timerBusy = false;

setInterval(async () => {
  if (!firebaseReady) return;

  if (!game) {
    await startGame();
    return;
  }

  sendClock();

  if (timerBusy) return;

  timerBusy = true;

  try {
    const now = Date.now();

    const currentRound = Number(
      game.roundNumber
    );

    const recorded = historyArray(
      game.history
    ).some(
      item =>
        Number(item.roundNumber) === currentRound
    );

    if (
      now >= Number(game.roundEndsAt) &&
      !recorded
    ) {
      await closeRound();
    }

    if (
      now >= cycleEnd(game) &&
      historyArray(game.history).some(
        item =>
          Number(item.roundNumber) ===
          Number(game.roundNumber)
      )
    ) {
      await nextRound();
    }
  } catch (error) {
    console.error(
      "GAME TIMER ERROR",
      error.message
    );
  } finally {
    timerBusy = false;
  }
}, 500);

setInterval(() => {
  settlePending().catch(error => {
    console.error(
      "SETTLEMENT TIMER ERROR",
      error.message
    );
  });
}, 1000);

// ===============================================
// HEALTH
// ===============================================

app.get("/", (req, res) => {
  const history = historyArray(
    game?.history
  );

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled:
      firebaseReady &&
      BETTING_ENABLED &&
      gamePhase(game) === "betting",
    giftsEnabled:
      firebaseReady &&
      GIFTS_ENABLED,
    roundNumber:
      game?.roundNumber ?? null,
    phase: gamePhase(game),
    secondsRemaining: remaining(),
    roundStartsAt:
      game?.roundStartsAt ?? null,
    roundEndsAt:
      game?.roundEndsAt ?? null,
    cycleEndsAt:
      game ? cycleEnd(game) : null,
    bettingDurationMs: BET_MS,
    resultDurationMs: RESULT_MS,
    lastResult:
      history.length > 0
        ? history[history.length - 1]
        : null
  });
});

// ===============================================
// SERVER START
// ===============================================

httpServer.listen(PORT, async () => {
  console.log(
    "MAX CHAT SERVER RUNNING",
    PORT
  );

  console.log(
    "BETTING ENABLED",
    BETTING_ENABLED
  );

  console.log(
    "GIFTS ENABLED",
    GIFTS_ENABLED
  );

  await startGame();

  if (game) {
    console.log(
      "GAME READY",
      game.roundNumber
    );
  }

  settlePending().catch(error => {
    console.error(
      "STARTUP SETTLEMENT ERROR",
      error.message
    );
  });
});
