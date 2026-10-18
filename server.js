
"use strict";

const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

// ======================================================
// MAX CHAT - SERVER
// Voice + Greedy Lion + Gifts + Firebase Wallet
// ======================================================

const app = express();
const httpServer = http.createServer(app);

const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  maxHttpBufferSize: 10 * 1024 * 1024
});

app.use(express.json({ limit: "16kb" }));

const PORT = Number(process.env.PORT || 10000);

const DATABASE_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SECRET_PATH =
  "/etc/secrets/firebase-service-account.json";

const GAME_PATH = "greedyLionGame";
const GAME_ROOM = "maxchat_greedy_lion_global";

const ROUND_DURATION = 25000;

const MULTIPLIERS = [
  45, 5, 5, 10, 15, 5, 5, 25
];

const BET_CHIPS = [
  100, 500, 1000, 5000, 10000
];

const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS !== "false";

const GIFTS_ENABLED =
  process.env.MAXCHAT_ENABLE_GIFTS !== "false";

// ======================================================
// GIFTS
// ======================================================

const GIFT_CATALOG = Object.freeze({
  rose: {
    name: "وردة ملكية",
    price: 100
  },
  heart: {
    name: "قلب الحب",
    price: 200
  },
  bear: {
    name: "دبدوب",
    price: 500
  },
  crown: {
    name: "تاج ملكي",
    price: 1000
  },
  bouquet: {
    name: "باقة ورد",
    price: 2000
  },
  car: {
    name: "سيارة فاخرة",
    price: 5000
  },
  yacht: {
    name: "يخت",
    price: 10000
  },
  palace: {
    name: "قصر",
    price: 20000
  },
  diamond: {
    name: "ألماس",
    price: 50000
  },
  horse: {
    name: "حصان ملكي",
    price: 100000
  },
  dragon: {
    name: "تنين أسطوري",
    price: 500000
  },
  galaxy: {
    name: "المجرة",
    price: 1000000
  },
  tree: {
    name: "شجرة الذهب",
    price: 2000000
  },
  throne: {
    name: "عرش ملكي",
    price: 5000000
  },
  kingdom: {
    name: "مملكة الذهب",
    price: 10000000
  }
});

// ======================================================
// FIREBASE INITIALIZATION
// ======================================================

let firebaseReady = false;

try {
  const credentials = JSON.parse(
    fs.readFileSync(SECRET_PATH, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(credentials),
    databaseURL: DATABASE_URL
  });

  firebaseReady = true;

  console.log("FIREBASE READY");
} catch (error) {
  console.error(
    "FIREBASE INITIALIZATION ERROR:",
    error.message
  );
}

function database() {
  if (!firebaseReady) {
    throw new Error("FIREBASE_NOT_READY");
  }

  return admin.database();
}

function usersRef() {
  return database().ref("Users");
}

function userRef(uid) {
  return usersRef().child(uid);
}

function gameRef() {
  return database().ref(GAME_PATH);
}

function safeInteger(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function validKey(value) {
  return (
    typeof value === "string" &&
    /^[A-Za-z0-9_-]{1,100}$/.test(value)
  );
}

function validRequestId(value) {
  return (
    typeof value === "string" &&
    /^[A-Za-z0-9_-]{16,100}$/.test(value)
  );
}

// ======================================================
// AUTHENTICATION
// ======================================================

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

// ======================================================
// WALLET
// IMPORTANT:
// Always read Firebase data BEFORE transactions.
// Firebase transaction callbacks can initially receive
// null even when the remote user exists.
// ======================================================

async function readWallet(uid, operation) {
  if (!validKey(uid)) {
    return {
      ok: false,
      error: "INVALID_UID"
    };
  }

  const snapshot = await userRef(uid).once("value");

  if (!snapshot.exists()) {
    console.error("WALLET USER NOT FOUND:", {
      operation,
      uid
    });

    return {
      ok: false,
      error: "USER_NOT_FOUND"
    };
  }

  const user = snapshot.val();

  if (!user || typeof user !== "object") {
    return {
      ok: false,
      error: "INVALID_USER"
    };
  }

  const coins = Number(user.coins);

  if (!safeInteger(coins)) {
    console.error("INVALID WALLET:", {
      operation,
      uid
    });

    return {
      ok: false,
      error: "INVALID_WALLET"
    };
  }

  console.log("WALLET VERIFIED:", {
    operation,
    uid,
    customId: user.customId || null,
    coins
  });

  return {
    ok: true,
    uid,
    coins,
    user
  };
}

// ======================================================
// GAME STATE
// ======================================================

let game = null;
let gameLoading = false;
let roundProcessing = false;
let settlementProcessing = false;
let lastSync = 0;

function isValidGame(value) {
  return Boolean(
    value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber > 0 &&
    Number.isFinite(Number(value.roundStartsAt)) &&
    Number.isFinite(Number(value.roundEndsAt))
  );
}

function normalizeHistory(value) {
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

function createGame(now) {
  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + ROUND_DURATION,
    activeBets: {},
    pendingSettlement: {},
    history: []
  };
}

function secondsRemaining() {
  if (!game) return null;

  return Math.max(
    0,
    Math.ceil(
      (Number(game.roundEndsAt) - Date.now()) /
        1000
    )
  );
}

function publicGameState() {
  return {
    roundNumber: game?.roundNumber ?? null,
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    serverTime: Date.now(),
    secondsRemaining: secondsRemaining(),
    bettingEnabled:
      BETTING_ENABLED && firebaseReady,
    phase: BETTING_ENABLED
      ? "betting"
      : "betting_disabled",
    multipliers: MULTIPLIERS,
    history: normalizeHistory(game?.history)
  };
}

function emitGameState() {
  if (!game) return;

  io.to(GAME_ROOM).emit(
    "greedy_lion_state",
    publicGameState()
  );
}

function emitGameClock() {
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
  if (!firebaseReady || gameLoading) return;

  gameLoading = true;

  try {
    const ref = gameRef();

    let snapshot = await ref.once("value");
    let saved = snapshot.val();

    if (!saved) {
      const now = Date.now();

      const tx = await ref.transaction(
        current => {
          if (current !== null) return;

          return createGame(now);
        },
        undefined,
        false
      );

      saved = tx.snapshot.val();

      if (!saved) {
        snapshot = await ref.once("value");
        saved = snapshot.val();
      }
    }

    if (!isValidGame(saved)) {
      console.error("INVALID GAME DATA");
      return;
    }

    game = saved;
    lastSync = Date.now();

    console.log(
      "GAME LOADED:",
      game.roundNumber
    );

    emitGameState();
  } catch (error) {
    console.error(
      "GAME LOAD ERROR:",
      error.message
    );
  } finally {
    gameLoading = false;
  }
}

// ======================================================
// ADVANCE GAME
// ======================================================

async function advanceGame() {
  if (!firebaseReady || roundProcessing) return;

  roundProcessing = true;

  try {
    const ref = gameRef();

    // Preload current state before transaction.
    const beforeSnapshot = await ref.once("value");

    if (!beforeSnapshot.exists()) {
      await loadGame();
      return;
    }

    const now = Date.now();
    const winningIndex = crypto.randomInt(8);

    const tx = await ref.transaction(
      current => {
        if (!isValidGame(current)) return;

        const start = Number(
          current.roundStartsAt
        );

        const end = Number(
          current.roundEndsAt
        );

        if (
          end - start !== ROUND_DURATION ||
          start > now + ROUND_DURATION
        ) {
          return {
            ...current,
            roundStartsAt: now,
            roundEndsAt: now + ROUND_DURATION
          };
        }

        if (Date.now() < end) return;

        const roundNumber =
          current.roundNumber;

        const bets =
          current.activeBets || {};

        const pending = {
          ...(current.pendingSettlement || {})
        };

        const result = {
          roundNumber,
          winningIndex,
          completedAt: now,
          hasSettledBets: false
        };

        if (Object.keys(bets).length > 0) {
          pending[String(roundNumber)] = {
            roundNumber,
            winningIndex,
            completedAt: now,
            payoutVersion: 2,
            settled: false,
            bets
          };
        }

        return {
          ...current,
          roundNumber: roundNumber + 1,
          roundStartsAt: now,
          roundEndsAt:
            now + ROUND_DURATION,
          activeBets: {},
          pendingSettlement: pending,
          history: [
            ...normalizeHistory(current.history),
            result
          ].slice(-8)
        };
      },
      undefined,
      false
    );

    const saved = tx.snapshot.val();

    if (!isValidGame(saved)) return;

    const oldRound =
      game?.roundNumber || 0;

    game = saved;
    lastSync = Date.now();

    if (saved.roundNumber > oldRound) {
      const completed =
        saved.roundNumber - 1;

      const result = normalizeHistory(
        saved.history
      ).find(
        item =>
          item.roundNumber === completed
      );

      if (result) {
        io.to(GAME_ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      console.log(
        "ROUND FINISHED:",
        completed
      );
    }

    emitGameState();
    emitGameClock();
  } catch (error) {
    console.error(
      "ROUND ERROR:",
      error.message
    );
  } finally {
    roundProcessing = false;
  }
}

// ======================================================
// RESERVE BET COINS
// ======================================================

async function reserveCoins(
  uid,
  betId,
  amount,
  roundNumber
) {
  const wallet = await readWallet(
    uid,
    "LION_BET"
  );

  if (!wallet.ok) {
    return wallet;
  }

  const ref = userRef(uid);

  let rejection = "WALLET_REJECTED";

  const tx = await ref.transaction(
    current => {
      if (!current) {
        rejection = "USER_NOT_FOUND";
        return;
      }

      const bets =
        current.lionBets || {};

      if (bets[betId]) {
        rejection = "DUPLICATE_BET";
        return;
      }

      const coins = Number(
        current.coins
      );

      if (!safeInteger(coins)) {
        rejection = "INVALID_WALLET";
        return;
      }

      if (coins < amount) {
        rejection = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...current,
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

  const saved = tx.snapshot.val();

  if (!tx.committed) {
    return {
      ok: false,
      error: rejection
    };
  }

  return {
    ok: true,
    balance: Number(saved.coins)
  };
}

// ======================================================
// BET STATUS
// ======================================================

async function setBetStatus(
  uid,
  betId,
  status
) {
  const ref = userRef(uid);

  const before = await ref.once("value");

  if (!before.child(`lionBets/${betId}`).exists()) {
    return false;
  }

  const tx = await ref.transaction(
    current => {
      const bet =
        current?.lionBets?.[betId];

      if (!bet) return;

      if (bet.status === status) return;

      if (bet.status !== "reserved") {
        return;
      }

      return {
        ...current,
        lionBets: {
          ...current.lionBets,
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

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]
      ?.status === status
  );
}

// ======================================================
// REFUND FAILED BET
// ======================================================

async function refundBet(uid, betId) {
  const ref = userRef(uid);

  const before = await ref.once("value");

  if (!before.child(`lionBets/${betId}`).exists()) {
    return false;
  }

  const tx = await ref.transaction(
    current => {
      const bet =
        current?.lionBets?.[betId];

      if (!bet) return;

      if (bet.status === "refunded") {
        return;
      }

      if (bet.status !== "reserved") {
        return;
      }

      const amount = Number(
        bet.amount
      );

      const coins = Number(
        current.coins
      );

      if (
        !safeInteger(amount) ||
        !safeInteger(coins) ||
        !Number.isSafeInteger(coins + amount)
      ) {
        return;
      }

      return {
        ...current,
        coins: coins + amount,
        lionBets: {
          ...current.lionBets,
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

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionBets?.[betId]
      ?.status === "refunded"
  );
}

// ======================================================
// PLACE BET
// ======================================================

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

  const token = String(
    data?.idToken || ""
  );

  const betId = String(
    data?.betId || ""
  );

  const foodIndex = Number(
    data?.foodIndex
  );

  const amount = Number(
    data?.amount
  );

  const roundNumber = Number(
    data?.roundNumber
  );

  if (
    !/^[a-f0-9]{32}$/i.test(betId) ||
    !Number.isInteger(foodIndex) ||
    foodIndex < 0 ||
    foodIndex > 7 ||
    !BET_CHIPS.includes(amount) ||
    !Number.isSafeInteger(roundNumber)
  ) {
    return {
      ok: false,
      error: "INVALID_BET"
    };
  }

  let decoded;

  try {
    decoded = await verifyToken(token);
  } catch (error) {
    return {
      ok: false,
      error: error.message
    };
  }

  const uid = decoded.uid;

  const betKey =
    `${uid}_${betId}`;

  const wallet = await readWallet(
    uid,
    "LION_BET_REQUEST"
  );

  if (!wallet.ok) {
    return wallet;
  }

  const currentGame = (
    await gameRef().once("value")
  ).val();

  if (
    !isValidGame(currentGame) ||
    currentGame.roundNumber !== roundNumber ||
    Date.now() >=
      Number(currentGame.roundEndsAt)
  ) {
    return {
      ok: false,
      error: "ROUND_CLOSED"
    };
  }

  // Prevent reusing an already processed bet.
  const previousBet =
    wallet.user.lionBets?.[betId];

  if (previousBet) {
    return {
      ok: false,
      error: "DUPLICATE_BET"
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
    console.error(
      "BET RESERVE ERROR:",
      error.message
    );

    return {
      ok: false,
      error: "WALLET_ERROR"
    };
  }

  if (!reserved.ok) {
    return reserved;
  }

  let rejection = "BET_REJECTED";

  try {
    const ref = gameRef();

    await ref.once("value");

    const tx = await ref.transaction(
      current => {
        if (!isValidGame(current)) {
          rejection = "GAME_NOT_READY";
          return;
        }

        if (
          current.roundNumber !== roundNumber ||
          Date.now() >=
            Number(current.roundEndsAt)
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

        for (
          const bet of Object.values(
            activeBets
          )
        ) {
          if (bet.uid === uid) {
            selected.add(
              Number(bet.foodIndex)
            );
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
      const refunded = await refundBet(
        uid,
        betId
      );

      return {
        ok: false,
        error: refunded
          ? rejection
          : "REFUND_PENDING"
      };
    }

    game = tx.snapshot.val();
    lastSync = Date.now();

    try {
      await setBetStatus(
        uid,
        betId,
        "accepted"
      );
    } catch (error) {
      console.error(
        "BET STATUS ERROR:",
        error.message
      );
    }

    console.log("BET ACCEPTED:", {
      uid,
      roundNumber,
      foodIndex,
      amount
    });

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
      "PLACE BET ERROR:",
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
          Boolean(
            entry?.bets?.[betKey]
          )
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

// ======================================================
// WINNER PAYOUT CALCULATION
// ======================================================

function calculatePayouts(entry) {
  const totals = {};

  const winner = Number(
    entry.winningIndex
  );

  for (
    const bet of Object.values(
      entry.bets || {}
    )
  ) {
    const uid = bet.uid;
    const foodIndex = Number(
      bet.foodIndex
    );

    const amount = Number(
      bet.amount
    );

    if (
      !validKey(uid) ||
      foodIndex !== winner ||
      !safeInteger(amount) ||
      amount === 0
    ) {
      continue;
    }

    const payout =
      amount * MULTIPLIERS[foodIndex];

    if (!safeInteger(payout)) {
      continue;
    }

    const total =
      (totals[uid] || 0) + payout;

    if (!safeInteger(total)) {
      continue;
    }

    totals[uid] = total;
  }

  return totals;
}

// ======================================================
// CREDIT WINNER
// ======================================================

async function creditWinner(
  uid,
  roundNumber,
  amount
) {
  const key = String(roundNumber);

  const ref = userRef(uid);

  const wallet = await readWallet(
    uid,
    "LION_PAYOUT"
  );

  if (!wallet.ok) return false;

  const tx = await ref.transaction(
    current => {
      if (!current) return;

      const payouts =
        current.lionPayouts || {};

      if (payouts[key]) {
        return;
      }

      const coins = Number(
        current.coins
      );

      if (
        !safeInteger(coins) ||
        !safeInteger(amount) ||
        amount <= 0 ||
        !Number.isSafeInteger(
          coins + amount
        )
      ) {
        return;
      }

      return {
        ...current,
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

  return Boolean(
    tx.committed ||
    tx.snapshot.val()?.lionPayouts?.[key]
  );
}

// ======================================================
// SETTLE FINISHED ROUNDS
// ======================================================

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

    const pending =
      snapshot.val() || {};

    for (
      const [key, entry] of Object.entries(
        pending
      )
    ) {
      if (!entry || entry.settled) {
        continue;
      }

      if (
        entry.payoutVersion !== 1 &&
        entry.payoutVersion !== 2
      ) {
        continue;
      }

      const totals =
        calculatePayouts(entry);

      let completed = true;

      for (
        const [uid, amount] of Object.entries(
          totals
        )
      ) {
        try {
          const credited =
            await creditWinner(
              uid,
              entry.roundNumber,
              amount
            );

          if (!credited) {
            completed = false;
          }
        } catch (error) {
          completed = false;

          console.error(
            "PAYOUT ERROR:",
            error.message
          );
        }
      }

      if (!completed) continue;

      await gameRef()
        .child("pendingSettlement")
        .child(key)
        .update({
          settled: true,
          settledAt: Date.now()
        });

      console.log(
        "ROUND SETTLED:",
        key
      );
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

// ======================================================
// GIFT RECIPIENT VALIDATION
// ======================================================

async function findGiftRecipient(
  roomId,
  receiverId
) {
  const snapshot = await database()
    .ref("rooms")
    .child(roomId)
    .child("seats")
    .once("value");

  let result = null;

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
      result = {
        userId,
        userUid
      };
    }
  });

  return result;
}

function sameGiftPayment(
  payment,
  request
) {
  return Boolean(
    payment &&
    payment.roomId === request.roomId &&
    payment.giftId === request.giftId &&
    payment.receiverId === request.receiverId &&
    payment.quantity === request.quantity &&
    payment.amount === request.amount
  );
}

// ======================================================
// CHARGE GIFT
// ======================================================

async function chargeGift(
  uid,
  request
) {
  const wallet = await readWallet(
    uid,
    "GIFT_SEND"
  );

  if (!wallet.ok) return wallet;

  const ref = userRef(uid);

  let rejection = "PAYMENT_REJECTED";

  const tx = await ref.transaction(
    current => {
      if (!current) {
        rejection = "USER_NOT_FOUND";
        return;
      }

      const payments =
        current.giftPayments || {};

      const previous =
        payments[request.requestId];

      if (previous) {
        rejection = sameGiftPayment(
          previous,
          request
        )
          ? "ALREADY_PAID"
          : "REQUEST_ID_CONFLICT";

        return;
      }

      const coins = Number(
        current.coins
      );

      if (!safeInteger(coins)) {
        rejection = "INVALID_WALLET";
        return;
      }

      if (coins < request.amount) {
        rejection = "INSUFFICIENT_COINS";
        return;
      }

      return {
        ...current,
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
    },
    undefined,
    false
  );

  const saved = tx.snapshot.val();

  const payment =
    saved?.giftPayments?.[
      request.requestId
    ];

  if (
    !sameGiftPayment(
      payment,
      request
    )
  ) {
    return {
      ok: false,
      error: rejection
    };
  }

  return {
    ok: true,
    newlyCharged: tx.committed,
    balance: Number(saved.coins),
    senderId: String(
      saved.customId || ""
    ),
    senderName: String(
      saved.name || "مستخدم"
    ),
    createdAt: payment.createdAt
  };
}

// ======================================================
// PUBLISH GIFT EVENT
// ======================================================

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

  const ref = database()
    .ref("rooms")
    .child(request.roomId)
    .child("giftEvents")
    .child(eventId);

  const before = await ref.once("value");

  if (!before.exists()) {
    await ref.transaction(
      current => {
        if (current !== null) return;

        return event;
      },
      undefined,
      false
    );
  }

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
    throw new Error(
      "GIFT_EVENT_CONFLICT"
    );
  }

  return saved;
}

// ======================================================
// HTTP HELPERS
// ======================================================

function sendError(
  res,
  status,
  error
) {
  return res.status(status).json({
    ok: false,
    error
  });
}

// ======================================================
// GIFT CATALOG
// ======================================================

app.get(
  "/api/gifts/catalog",
  (req, res) => {
    return res.json({
      ok: true,
      enabled:
        GIFTS_ENABLED && firebaseReady,
      gifts: GIFT_CATALOG
    });
  }
);

// ======================================================
// SEND GIFT
// ======================================================

app.post(
  "/api/gifts/send",
  async (req, res) => {
    if (!firebaseReady) {
      return sendError(
        res,
        503,
        "SERVER_NOT_READY"
      );
    }

    if (!GIFTS_ENABLED) {
      return sendError(
        res,
        503,
        "GIFTS_DISABLED"
      );
    }

    let decoded;

    try {
      decoded = await verifyHttpUser(
        req
      );
    } catch (error) {
      return sendError(
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

    const quantity =
      req.body?.quantity;

    if (
      !validKey(roomId) ||
      !validKey(giftId) ||
      !validKey(receiverId) ||
      !validRequestId(requestId) ||
      !Number.isSafeInteger(quantity) ||
      quantity < 1 ||
      quantity > 99
    ) {
      return sendError(
        res,
        400,
        "INVALID_GIFT_REQUEST"
      );
    }

    const gift =
      GIFT_CATALOG[giftId];

    if (!gift) {
      return sendError(
        res,
        400,
        "UNKNOWN_GIFT"
      );
    }

    const amount =
      gift.price * quantity;

    if (
      !safeInteger(amount) ||
      amount === 0
    ) {
      return sendError(
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
        return sendError(
          res,
          409,
          wallet.error
        );
      }

      const previous =
        wallet.user.giftPayments?.[
          requestId
        ];

      if (previous) {
        if (
          !sameGiftPayment(
            previous,
            request
          )
        ) {
          return sendError(
            res,
            409,
            "REQUEST_ID_CONFLICT"
          );
        }
      } else {
        const recipient =
          await findGiftRecipient(
            roomId,
            receiverId
          );

        if (!recipient) {
          return sendError(
            res,
            404,
            "RECEIVER_NOT_ON_SEAT"
          );
        }
      }

      const payment =
        await chargeGift(
          uid,
          request
        );

      if (!payment.ok) {
        const status =
          payment.error ===
          "INSUFFICIENT_COINS"
            ? 402
            : 409;

        return sendError(
          res,
          status,
          payment.error
        );
      }

      await publishGift(
        uid,
        request,
        payment
      );

      console.log(
        "GIFT SENT:",
        {
          uid,
          giftId,
          receiverId,
          amount,
          quantity
        }
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
        alreadyPaid:
          !payment.newlyCharged
      });
    } catch (error) {
      console.error(
        "GIFT ERROR:",
        error.message
      );

      return res.status(503).json({
        ok: false,
        error: "GIFT_STATUS_UNCERTAIN",
        requestId
      });
    }
  }
);

// ======================================================
// GAME CLOCK
// ======================================================

setInterval(() => {
  emitGameClock();

  if (!firebaseReady) return;

  if (!game) {
    if (!gameLoading) {
      loadGame().catch(console.error);
    }

    return;
  }

  const now = Date.now();

  const duration =
    Number(game.roundEndsAt) -
    Number(game.roundStartsAt);

  const expired =
    now >= Number(game.roundEndsAt);

  const invalidDuration =
    duration !== ROUND_DURATION;

  const invalidStart =
    Number(game.roundStartsAt) >
    now + ROUND_DURATION;

  const needsSync =
    now - lastSync >= 10000;

  if (
    (
      expired ||
      invalidDuration ||
      invalidStart ||
      needsSync
    ) &&
    !roundProcessing
  ) {
    advanceGame().catch(
      console.error
    );
  }
}, 1000);

// ======================================================
// PAYOUT SCHEDULER
// ======================================================

setInterval(() => {
  if (!settlementProcessing) {
    settlePending().catch(
      console.error
    );
  }
}, 5000);

// ======================================================
// SOCKET.IO
// ======================================================

io.on("connection", socket => {
  console.log(
    "USER CONNECTED:",
    socket.id
  );

  // ----------------------------------------------------
  // VOICE ROOM
  // ----------------------------------------------------

  socket.on("join_room", data => {
    if (!data?.roomId) return;

    socket.join(
      String(data.roomId)
    );

    socket.to(
      String(data.roomId)
    ).emit(
      "user_joined_room",
      {
        userId: data.userId,
        userName: data.userName
      }
    );
  });

  socket.on("toggle_mute", data => {
    if (!data?.roomId) return;

    io.to(
      String(data.roomId)
    ).emit(
      "user_toggle_mute",
      {
        userId: data.userId,
        isMuted: data.isMuted
      }
    );
  });

  socket.on("sit_on_seat", data => {
    if (!data?.roomId) return;

    io.to(
      String(data.roomId)
    ).emit(
      "user_sat_on_seat",
      {
        userId: data.userId,
        seatIndex: data.seatIndex
      }
    );
  });

  socket.on("leave_seat", data => {
    if (!data?.roomId) return;

    io.to(
      String(data.roomId)
    ).emit(
      "user_left_seat",
      {
        userId: data.userId
      }
    );
  });

  socket.on("start_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(
      String(data.roomId)
    ).emit(
      "user_started_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("stop_audio_stream", data => {
    if (!data?.roomId) return;

    socket.to(
      String(data.roomId)
    ).emit(
      "user_stopped_audio",
      {
        userId: data.userId
      }
    );
  });

  socket.on("voice_data", data => {
    if (!data?.roomId) return;

    socket.to(
      String(data.roomId)
    ).emit(
      "voice_data",
      {
        audioBuffer: data.audioBuffer,
        userId: data.userId
      }
    );
  });

  // ----------------------------------------------------
  // GREEDY LION JOIN
  // ----------------------------------------------------

  socket.on(
    "greedy_lion_join",
    async () => {
      socket.join(GAME_ROOM);

      if (!game) {
        await loadGame();
      }

      if (!game || !socket.connected) {
        return;
      }

      socket.emit(
        "greedy_lion_state",
        publicGameState()
      );

      socket.emit(
        "greedy_lion_clock",
        {
          roundNumber: game.roundNumber,
          roundStartsAt: game.roundStartsAt,
          roundEndsAt: game.roundEndsAt,
          serverTime: Date.now(),
          secondsRemaining:
            secondsRemaining()
        }
      );
    }
  );

  // ----------------------------------------------------
  // GREEDY LION BET
  // ----------------------------------------------------

  socket.on(
    "greedy_lion_place_bet",
    async (data, ack) => {
      const reply =
        typeof ack === "function"
          ? ack
          : () => {};

      try {
        const result =
          await placeBet(data);

        reply(result);
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

  socket.on(
    "greedy_lion_leave",
    () => {
      socket.leave(GAME_ROOM);
    }
  );

  socket.on("disconnect", () => {
    console.log(
      "USER DISCONNECTED:",
      socket.id
    );
  });
});

// ======================================================
// SERVER STATUS
// ======================================================

app.get("/", (req, res) => {
  const history = normalizeHistory(
    game?.history
  );

  res.json({
    app: "Max Chat",
    game: "Greedy Lion",
    status: "online",
    firebaseReady,
    bettingEnabled:
      BETTING_ENABLED && firebaseReady,
    giftsEnabled:
      GIFTS_ENABLED && firebaseReady,
    roundNumber:
      game?.roundNumber ?? null,
    secondsRemaining:
      secondsRemaining(),
    roundStartsAt:
      game?.roundStartsAt ?? null,
    roundEndsAt:
      game?.roundEndsAt ?? null,
    durationMs: game
      ? Number(game.roundEndsAt) -
        Number(game.roundStartsAt)
      : null,
    lastResult: history.length
      ? history[history.length - 1]
      : null
  });
});

// ======================================================
// START SERVER
// ======================================================

httpServer.listen(
  PORT,
  async () => {
    console.log(
      "MAX CHAT SERVER RUNNING:",
      PORT
    );

    console.log(
      "BETTING ENABLED:",
      BETTING_ENABLED
    );

    console.log(
      "GIFTS ENABLED:",
      GIFTS_ENABLED
    );

    await loadGame();

    if (game) {
      await advanceGame();
    }

    console.log(
      "GAME READY:",
      game?.roundNumber ??
        "NOT READY"
    );
  }
);
