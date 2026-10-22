'use strict';
// STAGING ONLY. The active server's real wallet is Users/{uid}/coins.
const {amount,createWallet,debit,credit} = require('./compact-wallet');
function createWalletStore(adminDatabase) {
  const refFor = uid => {if(!/^[a-zA-Z0-9_-]{1,100}$/.test(uid)) throw Error('INVALID_UID'); return adminDatabase.ref('walletV2').child(uid);};
  async function initIfEmpty(uid,verifiedSourceBalance) {
    const ref=refFor(uid);const initial=createWallet(verifiedSourceBalance);
    const tx=await ref.transaction(old=>old==null?initial:undefined,undefined,false);
    return {initialized:tx.committed,wallet:tx.snapshot.val()};
  }
  async function commit(uid,kind,id,value) {
    const ref=refFor(uid);const expected=String(amount(value));
    const tx=await ref.transaction(old=>{
      if(!old || typeof old.balance!=='string')return;
      const result=(kind==='debit'?debit:credit)(old,id,expected);
      return result.repeated?undefined:result.wallet;
    },undefined,false);
    if(!tx.snapshot.exists())throw Error('WALLET_NOT_MIGRATED');
    const saved=tx.snapshot.val();
    const from=kind==='debit'?'debits':'credits';
    if(saved?.[from]?.[id]!==expected)throw Error('WALLET_OPERATION_NOT_COMMITTED');
    return {balance:saved.balance,newlyApplied:tx.committed};
  }
  return {refFor,initIfEmpty,debit:(uid,id,value)=>commit(uid,'debit',id,value),credit:(uid,id,value)=>commit(uid,'credit',id,value)};
}
module.exports={createWalletStore};
