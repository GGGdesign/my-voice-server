'use strict';
// Server-only compact wallet implementation for staged migration.
// Safe amounts: nonnegative decimal strings, exact arithmetic with BigInt.
const AMOUNT_RE = /^(0|[1-9][0-9]{0,60})$/;
const MAX = 10n ** 60n - 1n;
function amount(x) {
  if (typeof x !== 'string' && typeof x !== 'bigint' && !(typeof x === 'number' && Number.isSafeInteger(x))) throw new Error('UNSAFE_AMOUNT');
  const s = String(x);
  if (!AMOUNT_RE.test(s)) throw new Error('INVALID_AMOUNT');
  const n = BigInt(s);
  if (n > MAX) throw new Error('AMOUNT_TOO_LARGE');
  return n;
}
function createWallet(initial) { return { balance: String(amount(initial)), version: 1, debits: {}, credits: {} }; }
function debit(wallet, opId, rawAmount) {
  if (!/^[a-zA-Z0-9_-]{16,100}$/.test(opId)) throw new Error('INVALID_OPERATION_ID');
  const a=amount(rawAmount); if (a===0n) throw new Error('ZERO_BET');
  if (wallet.debits?.[opId]) return { wallet, repeated:true };
  const bal=amount(wallet.balance);
  if (bal<a) throw new Error('INSUFFICIENT_COINS');
  return { wallet:{...wallet,balance:String(bal-a),debits:{...wallet.debits,[opId]:String(a)},version:(wallet.version||0)+1},repeated:false };
}
function credit(wallet, opId, rawAmount) {
  if (!/^[a-zA-Z0-9_-]{16,100}$/.test(opId)) throw new Error('INVALID_OPERATION_ID');
  const a=amount(rawAmount); if (a===0n) throw new Error('ZERO_PAYOUT');
  if (wallet.credits?.[opId]) return { wallet,repeated:true };
  const sum=amount(wallet.balance)+a;
  if (sum>MAX) throw new Error('BALANCE_TOO_LARGE');
  return {wallet:{...wallet,balance:String(sum),credits:{...wallet.credits,[opId]:String(a)},version:(wallet.version||0)+1},repeated:false};
}
function payoutForBets(bets, winner, multiplier) {
  if (!Number.isInteger(winner)||winner<0||winner>7) throw new Error('BAD_WINNER');
  const x=amount(multiplier); let total=0n;
  for(const b of bets){if(Number(b.foodIndex)===winner) total+=amount(b.amount)*x; if(total>MAX) throw new Error('PAYOUT_TOO_LARGE');}
  return String(total);
}
module.exports={amount,createWallet,debit,credit,payoutForBets};
