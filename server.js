
"use strict";

const express = require("express");
const http = require("http");
const fs = require("fs");
const crypto = require("crypto");
const admin = require("firebase-admin");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 10 * 1024 * 1024
});

app.use(express.json({ limit: "32kb" }));

const PORT = Number(process.env.PORT || 10000);

const DATABASE_URL =
  "https://maxvoice-a5e31-default-rtdb.europe-west1.firebasedatabase.app";

const SERVICE_ACCOUNT =
  "/etc/secrets/firebase-service-account.json";

const GAME_PATH = "greedyLionGame";
const GAME_ROOM = "maxchat_greedy_lion_global";

const BET_DURATION = 25000;
const RESULT_DURATION = 6000;

const MULTIPLIERS = [45, 5, 5, 10, 15, 5, 5, 25];
const CHIPS = [100, 500, 1000, 5000, 10000];

const BETTING_ENABLED =
  process.env.LION_ENABLE_BETS !== "false";

const GIFTS_ENABLED =
  process.env.MAXCHAT_ENABLE_GIFTS !== "false";

const GIFTS = {
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
};

let firebaseReady = false;

try {
  const account = JSON.parse(
    fs.readFileSync(SERVICE_ACCOUNT, "utf8")
  );

  admin.initializeApp({
    credential: admin.credential.cert(account),
    databaseURL: DATABASE_URL
  });

  firebaseReady = true;
  console.log("FIREBASE READY");
} catch (error) {
  console.error("FIREBASE ERROR", error.message);
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
  return (
    typeof value === "string" &&
    /^[a-zA-Z0-9_-]{1,100}$/.test(value)
  );
}

function validCoins(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function fail(error) {
  return { ok: false, error };
}

function transaction(ref, updater) {
  return ref.transaction(updater, undefined, false);
}

async function verifyToken(token) {
  if (!firebaseReady) {
    throw new Error("SERVER_NOT_READY");
  }

  if (!token || typeof token !== "string") {
    throw new Error("TOKEN_REQUIRED");
  }

  try {
    return await admin.auth().verifyIdToken(token);
  } catch {
    throw new Error("INVALID_TOKEN");
  }
}

function getName(user) {
  return String(
    user?.name ||
    user?.userName ||
    user?.username ||
    user?.nickname ||
    "مستخدم"
  );
}

function getPhoto(user) {
  return String(
    user?.profileImageUrl ||
    user?.profileImage ||
    user?.photoUrl ||
    user?.photoURL ||
    user?.avatarUrl ||
    user?.avatar ||
    user?.imageUrl ||
    user?.profilePicture ||
    user?.profilePhoto ||
    user?.picture ||
    ""
  );
}

function historyArray(value) {
  if (Array.isArray(value)) {
    return value.filter(Boolean);
  }

  if (value && typeof value === "object") {
    return Object.values(value)
      .filter(Boolean)
      .sort(
        (a, b) =>
          Number(a.roundNumber || 0) -
          Number(b.roundNumber || 0)
      );
  }

  return [];
}

// ==================================================
// GAME
// ==================================================

let game = null;
let gameListenerStarted = false;
let roundBusy = false;
let settlementBusy = false;

function validGame(value) {
  return Boolean(
    value &&
    Number.isSafeInteger(value.roundNumber) &&
    value.roundNumber > 0 &&
    Number.isFinite(Number(value.roundEndsAt))
  );
}

function createGame(now) {
  return {
    roundNumber: 1,
    roundStartsAt: now,
    roundEndsAt: now + BET_DURATION,
    cycleEndsAt:
      now + BET_DURATION + RESULT_DURATION,
    activeBets: {},
    pendingSettlement: {},
    history: []
  };
}

function cycleEnd(value) {
  return Number(
    value?.cycleEndsAt ||
    (Number(value?.roundEndsAt || 0) +
      RESULT_DURATION)
  );
}

function phase() {
  if (!validGame(game)) return "waiting";

  if (Date.now() < Number(game.roundEndsAt)) {
    return "betting";
  }

  return "result";
}

function secondsRemaining() {
  if (!game) return 0;

  const end =
    phase() === "betting"
      ? Number(game.roundEndsAt)
      : cycleEnd(game);

  return Math.max(
    0,
    Math.ceil((end - Date.now()) / 1000)
  );
}

function publicState() {
  return {
    roundNumber: game?.roundNumber ?? null,
    roundStartsAt: game?.roundStartsAt ?? null,
    roundEndsAt: game?.roundEndsAt ?? null,
    cycleEndsAt: game ? cycleEnd(game) : null,
    serverTime: Date.now(),
    secondsRemaining: secondsRemaining(),
    bettingEnabled:
      firebaseReady &&
      BETTING_ENABLED &&
      phase() === "betting",
    phase: phase(),
    multipliers: MULTIPLIERS,
    history: historyArray(game?.history).slice(-8)
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

async function startGame() {
  if (!firebaseReady || gameListenerStarted) {
    return;
  }

  gameListenerStarted = true;

  try {
    const ref = gameRef();
    const snapshot = await ref.once("value");

    if (!snapshot.exists()) {
      await transaction(ref, current => {
        if (current !== null) return;
        return createGame(Date.now());
      });
    }

    ref.on(
      "value",
      snapshot => {
        const value = snapshot.val();

        if (!validGame(value)) {
          console.error("INVALID GAME STATE");
          return;
        }

        game = value;
        sendState();
      },
      error => {
        console.error(
          "GAME LISTENER ERROR",
          error.message
        );
      }
    );

    console.log("GAME LISTENER READY");
  } catch (error) {
    gameListenerStarted = false;
    console.error(
      "GAME START ERROR",
      error.message
    );
  }
}

// ==================================================
// BET WALLET
// ==================================================

async function reserveBet(
  uid,
  betId,
  amount,
  roundNumber,
  foodIndex
) {
  let reason = "WALLET_REJECTED";

  const tx = await transaction(
    userRef(uid),
    user => {
      if (!user || typeof user !== "object") {
        reason = "USER_NOT_FOUND";
        return;
      }

      const coins = Number(user.coins);

      if (!validCoins(coins)) {
        reason = "INVALID_WALLET";
        return;
      }

      const bets = user.lionBets || {};

      if (bets[betId]) {
        reason = "DUPLICATE_BET";
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
            foodIndex,
            roundNumber,
            status: "reserved",
            createdAt: Date.now()
          }
        }
      };
    }
  );

  if (!tx.committed) {
    return fail(reason);
  }

  return {
    ok: true,
    balance: Number(tx.snapshot.val().coins)
  };
}

async function acceptBet(uid, betId) {
  const tx = await transaction(
    userRef(uid),
    user => {
      const bet = user?.lionBets?.[betId];

      if (!bet) return;

      if (bet.status === "accepted") return;

      if (bet.status !== "reserved") return;

      return {
        ...user,
        lionBets: {
          ...user.lionBets,
          [betId]: {
            ...bet,
            status: "accepted",
            acceptedAt: Date.now()
          }
        }
      };
    }
  );

  const status =
    tx.snapshot.val()?.lionBets?.[betId]?.status;

  return status === "accepted";
}

async function refundBet(uid, betId) {
  const tx = await transaction(
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
        !validCoins(coins + amount)
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

  return (
    tx.snapshot.val()?.lionBets?.[betId]
      ?.status === "refunded"
  );
}

async function findRecordedBet(uid, betId) {
  const key = `${uid}_${betId}`;
  const snapshot = await gameRef().once("value");
  const current = snapshot.val();

  if (current?.activeBets?.[key]) {
    return true;
  }

  for (const entry of Object.values(
    current?.pendingSettlement || {}
  )) {
    if (entry?.bets?.[key]) {
      return true;
    }
  }

  return false;
}

async function placeBet(data) {
  if (!firebaseReady || !game) {
    return fail("SERVER_NOT_READY");
  }

  if (!BETTING_ENABLED) {
    return fail("BETTING_DISABLED");
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
    return fail("INVALID_BET");
  }

  let decoded;

  try {
    decoded = await verifyToken(token);
  } catch (error) {
    return fail(error.message);
  }

  const uid = decoded.uid;
  const key = `${uid}_${betId}`;

  const walletSnapshot = await userRef(uid)
    .once("value");

  const wallet = walletSnapshot.val();

  if (!wallet) {
    return fail("USER_NOT_FOUND");
  }

  if (!validCoins(Number(wallet.coins))) {
    return fail("INVALID_WALLET");
  }

  const previous = wallet.lionBets?.[betId];

  if (previous) {
    if (
      Number(previous.amount) !== amount ||
      Number(previous.roundNumber) !== roundNumber
    ) {
      return fail("DUPLICATE_BET");
    }

    if (previous.status === "refunded") {
      return fail("BET_FAILED_REFUNDED");
    }

    if (
      previous.status === "accepted" ||
      await findRecordedBet(uid, betId)
    ) {
      return {
        ok: true,
        betId,
        foodIndex,
        amount,
        roundNumber,
        balance: Number(wallet.coins),
        alreadyAccepted: true
      };
    }

    return fail("BET_STATUS_UNCERTAIN");
  }

  const snapshot = await gameRef().once("value");
  const current = snapshot.val();

  if (
    !validGame(current) ||
    current.roundNumber !== roundNumber ||
    Date.now() >= Number(current.roundEndsAt)
  ) {
    return fail("ROUND_CLOSED");
  }

  const selected = new Set(
    Object.values(current.activeBets || {})
      .filter(bet => bet.uid === uid)
      .map(bet => Number(bet.foodIndex))
  );

  if (
    !selected.has(foodIndex) &&
    selected.size >= 6
  ) {
    return fail("MAX_SIX_SLOTS");
  }

  const reserved = await reserveBet(
    uid,
    betId,
    amount,
    roundNumber,
    foodIndex
  );

  if (!reserved.ok) {
    return reserved;
  }

  let rejection = "BET_REJECTED";

  try {
    const tx = await transaction(
      gameRef(),
      current => {
        if (!validGame(current)) {
          rejection = "GAME_NOT_READY";
          return;
        }

        if (
          current.roundNumber !== roundNumber ||
          Date.now() >= Number(current.roundEndsAt)
        ) {
          rejection = "ROUND_CLOSED";
          return;
        }

        const activeBets = {
          ...(current.activeBets || {})
        };

        if (activeBets[key]) {
          rejection = "DUPLICATE_BET";
          return;
        }

        const slots = new Set(
          Object.values(activeBets)
            .filter(bet => bet.uid === uid)
            .map(bet => Number(bet.foodIndex))
        );

        if (
          !slots.has(foodIndex) &&
          slots.size >= 6
        ) {
          rejection = "MAX_SIX_SLOTS";
          return;
        }

        activeBets[key] = {
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
      // Check the database before refunding.
      const recorded = await findRecordedBet(
        uid,
        betId
      );

      if (!recorded) {
        const refunded = await refundBet(
          uid,
          betId
        );

        return fail(
          refunded
            ? rejection
            : "BET_STATUS_UNCERTAIN"
        );
      }
    }

    await acceptBet(uid, betId);

    console.log(
      "BET ACCEPTED",
      uid,
      roundNumber,
      foodIndex,
      amount
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
    console.error(
      "BET SAVE ERROR",
      error.message
    );

    try {
      const recorded = await findRecordedBet(
        uid,
        betId
      );

      if (recorded) {
        await acceptBet(uid, betId);

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

      return fail(
        refunded
          ? "BET_FAILED_REFUNDED"
          : "BET_STATUS_UNCERTAIN"
      );
    } catch {
      return fail("BET_STATUS_UNCERTAIN");
    }
  }
}

// ==================================================
// ROUND RESULT
// ==================================================

async function closeRound() {
  if (!game || roundBusy) return;

  roundBusy = true;

  try {
    const roundNumber = Number(game.roundNumber);
    const winningIndex = crypto.randomInt(8);
    const now = Date.now();

    const tx = await transaction(
      gameRef(),
      current => {
        if (!validGame(current)) return;

        if (current.roundNumber !== roundNumber) {
          return;
        }

        if (Date.now() < current.roundEndsAt) {
          return;
        }

        const history = historyArray(
          current.history
        );

        if (
          history.some(
            item =>
              Number(item.roundNumber) ===
              roundNumber
          )
        ) {
          return;
        }

        const bets = current.activeBets || {};
        const pending = {
          ...(current.pendingSettlement || {})
        };

        const hasBets =
          Object.keys(bets).length > 0;

        if (hasBets) {
          pending[String(roundNumber)] = {
            roundNumber,
            winningIndex,
            completedAt: now,
            payoutVersion: 2,
            settled: false,
            bets
          };
        }

        const result = {
          roundNumber,
          winningIndex,
          completedAt: now,
          hasSettledBets: !hasBets,
          winners: [],
          topWinners: []
        };

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

    const result = historyArray(
      tx.snapshot.val().history
    ).find(
      item =>
        Number(item.roundNumber) === roundNumber
    );

    if (result) {
      io.to(GAME_ROOM).emit(
        "greedy_lion_round_result",
        result
      );
    }

    sendState();
    sendClock();

    settlePending().catch(console.error);
  } catch (error) {
    console.error(
      "CLOSE ROUND ERROR",
      error.message
    );
  } finally {
    roundBusy = false;
  }
}

async function advanceRound() {
  if (!game || roundBusy) return;

  roundBusy = true;

  try {
    const now = Date.now();

    const tx = await transaction(
      gameRef(),
      current => {
        if (!validGame(current)) return;

        if (now < cycleEnd(current)) {
          return;
        }

        const recorded = historyArray(
          current.history
        ).some(
          item =>
            Number(item.roundNumber) ===
            Number(current.roundNumber)
        );

        if (!recorded) return;

        return {
          ...current,
          roundNumber: current.roundNumber + 1,
          roundStartsAt: now,
          roundEndsAt: now + BET_DURATION,
          cycleEndsAt:
            now + BET_DURATION + RESULT_DURATION,
          activeBets: {}
        };
      }
    );

    if (tx.committed) {
      sendState();
      sendClock();
    }
  } catch (error) {
    console.error(
      "ADVANCE ROUND ERROR",
      error.message
    );
  } finally {
    roundBusy = false;
  }
}

// ==================================================
// PAYOUTS
// ==================================================

function calculatePayouts(entry) {
  const totals = {};
  const winningIndex = Number(
    entry.winningIndex
  );

  for (const bet of Object.values(
    entry.bets || {}
  )) {
    const uid = String(bet.uid || "");
    const index = Number(bet.foodIndex);
    const amount = Number(bet.amount);

    if (
      !validId(uid) ||
      index !== winningIndex ||
      !validCoins(amount) ||
      amount === 0
    ) {
      continue;
    }

    const payout =
      amount * MULTIPLIERS[index];

    if (!validCoins(payout)) continue;

    const total = (totals[uid] || 0) + payout;

    if (!validCoins(total)) continue;

    totals[uid] = total;
  }

  return totals;
}

async function creditWinner(
  uid,
  roundNumber,
  amount
) {
  const key = String(roundNumber);

  const tx = await transaction(
    userRef(uid),
    user => {
      if (!user) return;

      const payouts = user.lionPayouts || {};

      if (payouts[key]) return;

      const coins = Number(user.coins);

      if (
        !validCoins(coins) ||
        !validCoins(amount) ||
        !validCoins(coins + amount)
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

async function buildWinners(entry) {
  const totals = calculatePayouts(entry);

  const sorted = Object.entries(totals)
    .sort((a, b) => b[1] - a[1]);

  return Promise.all(
    sorted.map(async ([uid, amount], index) => {
      let user = {};

      try {
        const snapshot = await userRef(uid)
          .once("value");

        user = snapshot.val() || {};
      } catch (error) {
        console.error(
          "PROFILE ERROR",
          uid,
          error.message
        );
      }

      const name = getName(user);
      const photoUrl = getPhoto(user);

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
        winningIndex: Number(
          entry.winningIndex
        )
      };
    })
  );
}

// ==================================================
// SAVE WINNERS
// ==================================================

async function publishWinners(entry) {
  const roundNumber = Number(
    entry.roundNumber
  );

  const winners = await buildWinners(entry);

  const result = {
    roundNumber,
    winningIndex: Number(
      entry.winningIndex
    ),
    completedAt: Number(
      entry.completedAt
    ),
    hasSettledBets: true,
    winners,
    topWinners: winners.slice(0, 3)
  };

  // Store each round independently.
  // This prevents old rounds disappearing
  // from the short visible history.
  await gameRef()
    .child("roundResults")
    .child(String(roundNumber))
    .set(result);

  // Update visible history only if the round
  // is still inside the retained history.
  await transaction(
    gameRef(),
    current => {
      if (!validGame(current)) return;

      const history = historyArray(
        current.history
      );

      const index = history.findIndex(
        item =>
          Number(item.roundNumber) ===
          roundNumber
      );

      if (index < 0) {
        // Old round: standalone result was
        // already stored successfully.
        return;
      }

      if (
        history[index].hasSettledBets === true &&
        Array.isArray(history[index].winners)
      ) {
        return;
      }

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

async function settlePending() {
  if (!firebaseReady || settlementBusy) {
    return;
  }

  settlementBusy = true;

  try {
    const snapshot = await gameRef()
      .child("pendingSettlement")
      .once("value");

    const entries = snapshot.val() || {};

    for (const [key, entry] of Object.entries(
      entries
    )) {
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

        for (const [uid, amount] of Object.entries(
          totals
        )) {
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
              "PAYOUT ERROR",
              uid,
              error.message
            );
          }
        }

        if (!allPaid) continue;

        await gameRef()
          .child("pendingSettlement")
          .child(key)
          .update({
            settled: true,
            settledAt: Date.now()
          });
      }

      const savedResult = (
        await gameRef()
          .child("roundResults")
          .child(key)
          .once("value")
      ).val();

      if (
        !savedResult ||
        savedResult.hasSettledBets !== true
      ) {
        try {
          await publishWinners(entry);
        } catch (error) {
          console.error(
            "WINNER PUBLISH ERROR",
            key,
            error.message
          );

          continue;
        }
      }

      // The independent result has been saved.
      // The settled pending entry is no longer
      // needed and must not retry forever.
      await gameRef()
        .child("pendingSettlement")
        .child(key)
        .remove();

      console.log(
        "ROUND SETTLED",
        key
      );
    }
  } catch (error) {
    console.error(
      "SETTLEMENT ERROR",
      error.message
    );
  } finally {
    settlementBusy = false;
  }
}

// ==================================================
// GIFTS
// ==================================================

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
  let reason = "PAYMENT_REJECTED";

  const tx = await transaction(
    userRef(uid),
    user => {
      if (!user) {
        reason = "USER_NOT_FOUND";
        return;
      }

      const payments = user.giftPayments || {};
      const previous =
        payments[request.requestId];

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
            ...request,
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
    return fail(reason);
  }

  return {
    ok: true,
    newlyCharged: tx.committed,
    balance: Number(saved.coins),
    senderId: String(
      saved.customId || ""
    ),
    senderName: getName(saved),
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

  await transaction(ref, current => {
    if (current !== null) return;
    return event;
  });

  const saved = (
    await ref.once("value")
  ).val();

  if (
    !saved ||
    saved.senderUid !== uid ||
    saved.requestId !== request.requestId
  ) {
    throw new Error("GIFT_EVENT_NOT_SAVED");
  }

  return saved;
}

app.get("/api/gifts/catalog", (req, res) => {
  res.json({
    ok: true,
    enabled:
      firebaseReady && GIFTS_ENABLED,
    gifts: GIFTS
  });
});

app.post("/api/gifts/send", async (req, res) => {
  if (!firebaseReady || !GIFTS_ENABLED) {
    return res.status(503).json(
      fail("GIFTS_UNAVAILABLE")
    );
  }

  const header = String(
    req.headers.authorization || ""
  );

  const match = /^Bearer\s+(.+)$/i.exec(
    header
  );

  if (!match) {
    return res.status(401).json(
      fail("TOKEN_REQUIRED")
    );
  }

  let decoded;

  try {
    decoded = await verifyToken(match[1]);
  } catch (error) {
    return res.status(401).json(
      fail(error.message)
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

  const quantity = Number(
    req.body?.quantity
  );

  if (
    !validId(roomId) ||
    !validId(giftId) ||
    !validId(receiverId) ||
    !/^[a-zA-Z0-9_-]{16,100}$/.test(requestId) ||
    !Number.isSafeInteger(quantity) ||
    quantity < 1 ||
    quantity > 99
  ) {
    return res.status(400).json(
      fail("INVALID_GIFT_REQUEST")
    );
  }

  const gift = GIFTS[giftId];

  if (!gift) {
    return res.status(400).json(
      fail("UNKNOWN_GIFT")
    );
  }

  const amount = gift.price * quantity;

  if (!validCoins(amount)) {
    return res.status(400).json(
      fail("INVALID_AMOUNT")
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
    const snapshot = await userRef(uid)
      .once("value");

    const user = snapshot.val();

    if (!user) {
      return res.status(404).json(
        fail("USER_NOT_FOUND")
      );
    }

    const previous =
      user.giftPayments?.[requestId];

    if (!previous) {
      const onSeat =
        await giftReceiverOnSeat(
          roomId,
          receiverId
        );

      if (!onSeat) {
        return res.status(404).json(
          fail("RECEIVER_NOT_ON_SEAT")
        );
      }
    } else if (
      !sameGiftPayment(previous, request)
    ) {
      return res.status(409).json(
        fail("REQUEST_ID_CONFLICT")
      );
    }

    const payment = await chargeGift(
      uid,
      request
    );

    if (!payment.ok) {
      return res.status(409).json(payment);
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
      "GIFT ERROR",
      error.message
    );

    return res.status(503).json({
      ok: false,
      error: "GIFT_STATUS_UNCERTAIN",
      requestId
    });
  }
});

// ==================================================
// SOCKET EVENTS
// ==================================================

io.on("connection", socket => {
  console.log(
    "CONNECTED",
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

  socket.on(
    "greedy_lion_join",
    async () => {
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
          secondsRemaining:
            state.secondsRemaining,
          bettingEnabled:
            state.bettingEnabled,
          phase: state.phase
        }
      );
    }
  );

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
          "BET SOCKET ERROR",
          error.message
        );

        reply(fail("SERVER_ERROR"));
      }
    }
  );

  socket.on("greedy_lion_leave", () => {
    socket.leave(GAME_ROOM);
  });

  socket.on("disconnect", () => {
    console.log(
      "DISCONNECTED",
      socket.id
    );
  });
});

// ==================================================
// TIMERS
// ==================================================

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

    const recorded = historyArray(
      game.history
    ).some(
      item =>
        Number(item.roundNumber) ===
        Number(game.roundNumber)
    );

    if (
      now >= Number(game.roundEndsAt) &&
      !recorded
    ) {
      await closeRound();
    } else if (
      now >= cycleEnd(game) &&
      recorded
    ) {
      await advanceRound();
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
}, 2000);

// ==================================================
// HEALTH
// ==================================================

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
      phase() === "betting",
    giftsEnabled:
      firebaseReady && GIFTS_ENABLED,
    roundNumber:
      game?.roundNumber ?? null,
    phase: phase(),
    secondsRemaining:
      secondsRemaining(),
    roundStartsAt:
      game?.roundStartsAt ?? null,
    roundEndsAt:
      game?.roundEndsAt ?? null,
    cycleEndsAt:
      game ? cycleEnd(game) : null,
    lastResult:
      history.length > 0
        ? history[history.length - 1]
        : null
  });
});

// ==================================================
// START
// ==================================================

server.listen(PORT, async () => {
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

  settlePending().catch(error => {
    console.error(
      "STARTUP SETTLEMENT ERROR",
      error.message
    );
  });
});
