
"use strict";

// Max Chat Greedy Lion
// Used by server.js through attachGreedyLion.

function attachGreedyLion({
  app,
  io,
  admin,
  firebaseReady
}) {
  const crypto = require("crypto");

  const ROOM = "maxchat_greedy_lion_global";
  const PATH = "greedyLionGame";

  const BET_MS = 25000;
  const RESULT_MS = 6000;

  const MIN_BET = 100;
  const MAX_BET = 100000000;

  const MULTIPLIERS = [
    45, 5, 5, 10, 15, 5, 5, 25
  ];

  const enabled =
    process.env.LION_ENABLE_BETS !== "false";

  const ready = Boolean(firebaseReady);

  const db = () => admin.database();

  const gameRef = () => db().ref(PATH);

  const userRef = uid =>
    db().ref("Users").child(uid);

  const validCoins = n =>
    Number.isSafeInteger(n) && n >= 0;

  const validId = s =>
    typeof s === "string" &&
    /^[a-zA-Z0-9_-]{1,100}$/.test(s);

  const fail = error => ({
    ok: false,
    error
  });

  let game = null;
  let starting = false;
  let switching = false;
  let settling = false;
  let lastStateSignature = "";

  async function transaction(ref, callback) {
    // Atomic Firebase transaction.
    // No network calls inside updater.
    return ref.transaction(
      callback,
      undefined,
      false
    );
  }

  function historyArray(value) {
    const array = Array.isArray(value)
      ? value
      : Object.values(value || {});

    return array
      .filter(Boolean)
      .sort(
        (a, b) =>
          Number(a.roundNumber) -
          Number(b.roundNumber)
      )
      .slice(-12);
  }

  function validGame(g) {
    return !!g &&
      Number.isSafeInteger(g.roundNumber) &&
      g.roundNumber > 0 &&
      Number.isFinite(Number(g.roundEndsAt)) &&
      Number.isFinite(Number(g.roundStartsAt));
  }

  const cycleEnd = g =>
    Number(
      g.cycleEndsAt ||
      (Number(g.roundEndsAt) + RESULT_MS)
    );

  function phase(g) {
    if (!validGame(g)) {
      return "waiting";
    }

    return Date.now() < Number(g.roundEndsAt)
      ? "betting"
      : "result";
  }

  function makeGame(now) {
    return {
      roundNumber: 1,
      roundStartsAt: now,
      roundEndsAt: now + BET_MS,
      cycleEndsAt: now + BET_MS + RESULT_MS,
      activeBets: {},
      pendingSettlement: {},
      history: []
    };
  }

  function state() {
    const p = phase(game);

    const end = p === "betting"
      ? game?.roundEndsAt
      : (game ? cycleEnd(game) : 0);

    return {
      roundNumber: game?.roundNumber || null,
      roundStartsAt: game?.roundStartsAt || null,
      roundEndsAt: game?.roundEndsAt || null,
      cycleEndsAt: game
        ? cycleEnd(game)
        : null,
      serverTime: Date.now(),
      secondsRemaining: Math.max(
        0,
        Math.ceil(
          (Number(end) - Date.now()) / 1000
        )
      ),
      bettingEnabled:
        ready && enabled && p === "betting",
      phase: p,
      minBet: MIN_BET,
      maxBet: MAX_BET,
      multipliers: MULTIPLIERS,
      history: historyArray(game?.history)
    };
  }

  function sendState(force = false) {
    if (!game) return;

    const s = state();

    const signature = JSON.stringify({
      round: s.roundNumber,
      phase: s.phase,
      history: s.history
    });

    if (
      !force &&
      signature === lastStateSignature
    ) {
      return;
    }

    lastStateSignature = signature;

    io.to(ROOM).emit(
      "greedy_lion_state",
      s
    );
  }

  function sendClock() {
    if (!game) return;

    const s = state();

    io.to(ROOM).emit(
      "greedy_lion_clock",
      {
        roundNumber: s.roundNumber,
        roundStartsAt: s.roundStartsAt,
        roundEndsAt: s.roundEndsAt,
        cycleEndsAt: s.cycleEndsAt,
        serverTime: s.serverTime,
        secondsRemaining: s.secondsRemaining,
        phase: s.phase,
        bettingEnabled: s.bettingEnabled
      }
    );
  }

  async function startGame() {
    if (!ready || starting) return;

    starting = true;

    try {
      const ref = gameRef();

      await transaction(
        ref,
        current =>
          current === null
            ? makeGame(Date.now())
            : undefined
      );

      ref.on(
        "value",
        snap => {
          const current = snap.val();

          if (!validGame(current)) {
            return;
          }

          game = current;
          sendState();
        },
        error => {
          console.error(
            "LION GAME LISTENER:",
            error.message
          );
        }
      );

      console.log("GREEDY LION READY");
    } catch (error) {
      starting = false;
      console.error(
        "LION START ERROR:",
        error.message
      );
    }
  }

  async function verifyToken(token) {
    if (!ready) {
      throw Error("SERVER_NOT_READY");
    }

    if (
      !token ||
      typeof token !== "string"
    ) {
      throw Error("TOKEN_REQUIRED");
    }

    try {
      return await admin.auth()
        .verifyIdToken(token, true);
    } catch {
      throw Error("INVALID_TOKEN");
    }
  }

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

  // =====================================
  // EXACT WINNINGS
  // =====================================

  function payouts(entry) {
    const result = {};

    const winningIndex = Number(
      entry.winningIndex
    );

    if (
      !(
        winningIndex >= 0 &&
        winningIndex < 8
      )
    ) {
      return result;
    }

    for (
      const bet of Object.values(
        entry.bets || {}
      )
    ) {
      const uid = bet?.uid;
      const amount = Number(bet?.amount);

      if (
        !validId(uid) ||
        Number(bet.foodIndex) !== winningIndex ||
        !validCoins(amount) ||
        amount === 0
      ) {
        continue;
      }

      const payout =
        amount * MULTIPLIERS[winningIndex];

      const next =
        (result[uid] || 0) + payout;

      if (
        !validCoins(payout) ||
        !validCoins(next)
      ) {
        throw Error(
          "PAYOUT_OVERFLOW:" + uid
        );
      }

      result[uid] = next;
    }

    return result;
  }

  // =====================================
  // REAL WINNER PROFILES
  // =====================================

  async function winnerDetails(entry) {
    const all = Object.entries(
      payouts(entry)
    ).sort(
      (a, b) => b[1] - a[1]
    );

    return Promise.all(
      all.map(
        async ([uid, amount], idx) => {
          let user = {};

          try {
            user = (
              await userRef(uid)
                .once("value")
            ).val() || {};
          } catch (error) {
            console.error(
              "WINNER PROFILE:",
              uid,
              error.message
            );
          }

          const name =
            profileName(user);

          const photoUrl =
            profilePhoto(user);

          return {
            rank: idx + 1,
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
        }
      )
    );
  }

  async function publishWinners(entry) {
    const roundNumber = Number(
      entry.roundNumber
    );

    const winners =
      await winnerDetails(entry);

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
      topWinners:
        winners.slice(0, 3)
    };

    const tx = await transaction(
      gameRef(),
      current => {
        if (!validGame(current)) {
          return;
        }

        const history = historyArray(
          current.history
        );

        const i = history.findIndex(
          x =>
            Number(x.roundNumber) ===
            roundNumber
        );

        if (i < 0) {
          return;
        }

        if (
          history[i].hasSettledBets &&
          Array.isArray(
            history[i].winners
          ) &&
          JSON.stringify(
            history[i].winners
          ) === JSON.stringify(winners)
        ) {
          return;
        }

        history[i] = {
          ...history[i],
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
      x =>
        Number(x.roundNumber) ===
        roundNumber
    );

    if (!saved?.hasSettledBets) {
      throw Error(
        "RESULT_NOT_SAVED"
      );
    }

    io.to(ROOM).emit(
      "greedy_lion_winners",
      result
    );

    io.to(ROOM).emit(
      "greedy_lion_round_result",
      result
    );

    return result;
  }

  // =====================================
  // END OF BETTING
  // =====================================

  async function announceResult() {
    if (
      !ready ||
      !game ||
      switching
    ) {
      return;
    }

    switching = true;

    try {
      const roundNumber = Number(
        game.roundNumber
      );

      const winningIndex =
        crypto.randomInt(8);

      const now = Date.now();

      const tx = await transaction(
        gameRef(),
        current => {
          if (
            !validGame(current) ||
            Number(
              current.roundNumber
            ) !== roundNumber ||
            Date.now() <
              Number(
                current.roundEndsAt
              )
          ) {
            return;
          }

          const history = historyArray(
            current.history
          );

          if (
            history.some(
              x =>
                Number(x.roundNumber) ===
                roundNumber
            )
          ) {
            return;
          }

          const bets =
            current.activeBets || {};

          const pendingSettlement = {
            ...(
              current.pendingSettlement || {}
            )
          };

          if (
            Object.keys(bets).length
          ) {
            pendingSettlement[
              String(roundNumber)
            ] = {
              roundNumber,
              winningIndex,
              completedAt: now,
              payoutVersion: 3,
              settled: false,
              bets
            };
          }

          const result = {
            roundNumber,
            winningIndex,
            completedAt: now,
            hasSettledBets:
              !Object.keys(bets).length,
            winners: [],
            topWinners: []
          };

          return {
            ...current,
            pendingSettlement,
            history: [
              ...history,
              result
            ].slice(-12)
          };
        }
      );

      if (!tx.committed) {
        return;
      }

      game = tx.snapshot.val();

      const result = historyArray(
        game.history
      ).find(
        x =>
          Number(x.roundNumber) ===
          roundNumber
      );

      if (result) {
        io.to(ROOM).emit(
          "greedy_lion_round_result",
          result
        );
      }

      sendState(true);

      settlePending().catch(
        error => {
          console.error(
            "SETTLE AFTER RESULT:",
            error.message
          );
        }
      );
    } catch (error) {
      console.error(
        "LION RESULT ERROR:",
        error.message
      );
    } finally {
      switching = false;
    }
  }

  // =====================================
  // NEXT ROUND
  // =====================================

  async function nextRound() {
    if (
      !ready ||
      !game ||
      switching
    ) {
      return;
    }

    switching = true;

    try {
      const now = Date.now();

      const tx = await transaction(
        gameRef(),
        current => {
          if (
            !validGame(current) ||
            Date.now() < cycleEnd(current)
          ) {
            return;
          }

          const roundNumber = Number(
            current.roundNumber
          );

          const recorded = historyArray(
            current.history
          ).some(
            x =>
              Number(x.roundNumber) ===
              roundNumber
          );

          if (!recorded) {
            return;
          }

          return {
            ...current,
            roundNumber:
              roundNumber + 1,
            roundStartsAt: now,
            roundEndsAt:
              now + BET_MS,
            cycleEndsAt:
              now + BET_MS + RESULT_MS,
            activeBets: {}
          };
        }
      );

      if (!tx.committed) {
        return;
      }

      game = tx.snapshot.val();

      sendState(true);
      sendClock();
    } catch (error) {
      console.error(
        "NEXT ROUND ERROR:",
        error.message
      );
    } finally {
      switching = false;
    }
  }

  // =====================================
  // RESERVE REAL COINS
  // =====================================

  async function reserveBet(
    uid,
    betId,
    amount,
    roundNumber,
    foodIndex
  ) {
    let reason =
      "BET_REJECTED";

    const tx = await transaction(
      userRef(uid),
      user => {
        if (!user) {
          reason = "USER_NOT_FOUND";
          return;
        }

        const old =
          user.lionBets || {};

        if (old[betId]) {
          reason = "DUPLICATE_BET";
          return;
        }

        const coins =
          Number(user.coins);

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
            ...old,
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
      balance: Number(
        tx.snapshot.val().coins
      )
    };
  }

  async function setBetStatus(
    uid,
    betId
  ) {
    await transaction(
      userRef(uid),
      user => {
        if (
          user?.lionBets?.[betId]
            ?.status !== "reserved"
        ) {
          return;
        }

        return {
          ...user,
          lionBets: {
            ...user.lionBets,
            [betId]: {
              ...user.lionBets[betId],
              status: "accepted"
            }
          }
        };
      }
    );
  }

  async function refundBet(
    uid,
    betId
  ) {
    const tx = await transaction(
      userRef(uid),
      user => {
        const bet =
          user?.lionBets?.[betId];

        if (
          !bet ||
          bet.status !== "reserved"
        ) {
          return;
        }

        const coins =
          Number(user.coins);

        const amount =
          Number(bet.amount);

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
      tx.snapshot.val()
        ?.lionBets?.[betId]
        ?.status === "refunded"
    );
  }

  async function betRecorded(key) {
    const saved = (
      await gameRef().once("value")
    ).val();

    return Boolean(
      saved?.activeBets?.[key] ||
      Object.values(
        saved?.pendingSettlement || {}
      ).some(
        e => e?.bets?.[key]
      )
    );
  }

  // =====================================
  // PLACE BET
  // =====================================

  async function placeBet(data) {
    if (!ready || !game) {
      return fail("SERVER_NOT_READY");
    }

    if (!enabled) {
      return fail("BETTING_DISABLED");
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
      foodIndex >= 8 ||
      !Number.isSafeInteger(amount) ||
      amount < MIN_BET ||
      amount > MAX_BET ||
      !Number.isSafeInteger(roundNumber)
    ) {
      return fail("INVALID_BET");
    }

    let uid;

    try {
      uid = (
        await verifyToken(token)
      ).uid;
    } catch (error) {
      return fail(error.message);
    }

    if (!validId(uid)) {
      return fail("INVALID_UID");
    }

    const key =
      `${uid}_${betId}`;

    const saved = (
      await gameRef().once("value")
    ).val();

    if (
      !validGame(saved) ||
      saved.roundNumber !== roundNumber ||
      phase(saved) !== "betting"
    ) {
      return fail("ROUND_CLOSED");
    }

    let reservation;

    try {
      reservation = await reserveBet(
        uid,
        betId,
        amount,
        roundNumber,
        foodIndex
      );
    } catch (error) {
      console.error(
        "RESERVE ERROR:",
        error.message
      );

      return fail("WALLET_ERROR");
    }

    if (!reservation.ok) {
      return reservation;
    }

    let reason = "BET_REJECTED";

    try {
      const tx = await transaction(
        gameRef(),
        current => {
          if (
            !validGame(current) ||
            current.roundNumber !==
              roundNumber ||
            phase(current) !== "betting"
          ) {
            reason = "ROUND_CLOSED";
            return;
          }

          const activeBets = {
            ...(
              current.activeBets || {}
            )
          };

          if (activeBets[key]) {
            reason = "DUPLICATE_BET";
            return;
          }

          // Repeated bets allowed.
          // All 8 slots allowed.
          // Real wallet is the limit.

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
        const refunded =
          await refundBet(
            uid,
            betId
          );

        return fail(
          refunded
            ? reason
            : "REFUND_PENDING"
        );
      }

      try {
        await setBetStatus(
          uid,
          betId
        );
      } catch (error) {
        console.error(
          "SET BET STATUS:",
          error.message
        );
      }

      return {
        ok: true,
        betId,
        foodIndex,
        amount,
        roundNumber,
        balance: reservation.balance
      };
    } catch (error) {
      console.error(
        "PLACE BET:",
        error.message
      );

      try {
        if (
          await betRecorded(key)
        ) {
          return {
            ok: true,
            betId,
            foodIndex,
            amount,
            roundNumber,
            balance: reservation.balance
          };
        }

        const refunded =
          await refundBet(
            uid,
            betId
          );

        return fail(
          refunded
            ? "BET_FAILED_REFUNDED"
            : "BET_STATUS_UNCERTAIN"
        );
      } catch {
        return fail(
          "BET_STATUS_UNCERTAIN"
        );
      }
    }
  }

  // =====================================
  // PAY WINNER ONCE
  // =====================================

  async function payWinner(
    uid,
    roundNumber,
    amount
  ) {
    const key =
      String(roundNumber);

    const tx = await transaction(
      userRef(uid),
      user => {
        if (!user) {
          return;
        }

        if (
          user.lionPayouts?.[key]
        ) {
          return;
        }

        const coins =
          Number(user.coins);

        if (
          !validCoins(coins) ||
          !validCoins(amount) ||
          amount <= 0 ||
          !validCoins(coins + amount)
        ) {
          return;
        }

        return {
          ...user,
          coins: coins + amount,
          lionPayouts: {
            ...(
              user.lionPayouts || {}
            ),
            [key]: {
              amount,
              completedAt: Date.now()
            }
          }
        };
      }
    );

    const paid = tx.snapshot
      .val()?.lionPayouts?.[key];

    if (
      paid &&
      Number(paid.amount) !== amount
    ) {
      console.error(
        "PAYOUT MISMATCH:",
        uid,
        roundNumber,
        paid.amount,
        amount
      );

      return false;
    }

    return Boolean(paid);
  }

  // =====================================
  // SETTLEMENT
  // =====================================

  async function settlePending() {
    if (
      !ready ||
      !game ||
      settling
    ) {
      return;
    }

    settling = true;

    try {
      const pending = (
        await gameRef()
          .child("pendingSettlement")
          .once("value")
      ).val() || {};

      for (
        const [key, entry] of
        Object.entries(pending)
      ) {
        if (!entry) {
          continue;
        }

        if (
          ![1, 2, 3].includes(
            Number(entry.payoutVersion)
          )
        ) {
          continue;
        }

        if (!entry.settled) {
          let paidAll = true;
          let totals;

          try {
            totals = payouts(entry);
          } catch (error) {
            console.error(
              "INVALID PAYOUT:",
              key,
              error.message
            );

            continue;
          }

          for (
            const [uid, amount] of
            Object.entries(totals)
          ) {
            try {
              if (
                !(
                  await payWinner(
                    uid,
                    entry.roundNumber,
                    amount
                  )
                )
              ) {
                paidAll = false;
              }
            } catch (error) {
              paidAll = false;

              console.error(
                "PAYOUT ERROR:",
                uid,
                error.message
              );
            }
          }

          if (!paidAll) {
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

        const history = historyArray(
          (
            await gameRef()
              .child("history")
              .once("value")
          ).val()
        );

        const published =
          history.some(
            x =>
              Number(x.roundNumber) ===
                Number(entry.roundNumber) &&
              x.hasSettledBets === true
          );

        if (!published) {
          try {
            await publishWinners(entry);
          } catch (error) {
            console.error(
              "WINNERS PUBLISH:",
              key,
              error.message
            );
          }
        }

        // Retain recovery data for 12 rounds
        // before cleaning completed settlements.

        const currentRound =
          Number(game?.roundNumber || 0);

        if (
          published &&
          Number(entry.roundNumber) <
            currentRound - 12
        ) {
          await gameRef()
            .child("pendingSettlement")
            .child(key)
            .remove();
        }
      }
    } catch (error) {
      console.error(
        "SETTLEMENT ERROR:",
        error.message
      );
    } finally {
      settling = false;
    }
  }

  // =====================================
  // SOCKET GAME EVENTS
  // =====================================

  io.on("connection", socket => {
    socket.on(
      "greedy_lion_join",
      async () => {
        socket.join(ROOM);

        if (!game) {
          await startGame();
        }

        if (
          !socket.connected ||
          !game
        ) {
          return;
        }

        const s = state();

        socket.emit(
          "greedy_lion_state",
          s
        );

        socket.emit(
          "greedy_lion_clock",
          {
            roundNumber: s.roundNumber,
            roundStartsAt: s.roundStartsAt,
            roundEndsAt: s.roundEndsAt,
            cycleEndsAt: s.cycleEndsAt,
            serverTime: s.serverTime,
            secondsRemaining:
              s.secondsRemaining,
            phase: s.phase,
            bettingEnabled:
              s.bettingEnabled
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
          reply(
            await placeBet(data)
          );
        } catch (error) {
          console.error(
            "SOCKET BET ERROR:",
            error.message
          );

          reply(
            fail("SERVER_ERROR")
          );
        }
      }
    );

    socket.on(
      "greedy_lion_leave",
      () => {
        socket.leave(ROOM);
      }
    );
  });

  // =====================================
  // TIMERS
  // =====================================

  setInterval(() => {
    if (!ready) {
      return;
    }

    if (!game) {
      startGame().catch(
        console.error
      );
      return;
    }

    sendClock();

    if (switching) {
      return;
    }

    const recorded =
      historyArray(game.history)
        .some(
          x =>
            Number(x.roundNumber) ===
            Number(game.roundNumber)
        );

    if (
      Date.now() >=
        Number(game.roundEndsAt) &&
      !recorded
    ) {
      announceResult().catch(
        console.error
      );
    } else if (
      Date.now() >=
        cycleEnd(game) &&
      recorded
    ) {
      nextRound().catch(
        console.error
      );
    }
  }, 500);

  setInterval(() => {
    settlePending().catch(
      console.error
    );
  }, 1000);

  // =====================================
  // GAME STATUS
  // =====================================

  app.get(
    "/api/greedy-lion/status",
    (req, res) => {
      const history =
        historyArray(game?.history);

      res.json({
        app: "Max Chat",
        service: "greedy_lion_only",
        status: "online",
        firebaseReady: ready,
        roundNumber:
          game?.roundNumber || null,
        phase: phase(game),
        secondsRemaining:
          state().secondsRemaining,
        bettingEnabled:
          ready &&
          enabled &&
          phase(game) === "betting",
        minBet: MIN_BET,
        maxBet: MAX_BET,
        bettingDurationMs: BET_MS,
        resultDurationMs: RESULT_MS,
        lastResult:
          history.length
            ? history[history.length - 1]
            : null
      });
    }
  );

  console.log(
    "GREEDY LION MODULE ATTACHED"
  );

  startGame()
    .then(() => {
      settlePending().catch(
        console.error
      );
    })
    .catch(console.error);
}

module.exports = {
  attachGreedyLion
};
