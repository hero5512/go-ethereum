package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"math/big"
	"sort"
)

// DiffDb is a database for storing state diffs per block
type TxDB interface {
	InsertTx(txHash, tx string) error
	Close() error
	ForceCommit() error
}

type DiffStateDb struct {
	*StateDB
	LocalObject map[common.Address]*LocalObject
}

func NewDiffDb(stateDb *StateDB) *DiffStateDb {
	diffDb := &DiffStateDb{
		StateDB:     stateDb,
		LocalObject: make(map[common.Address]*LocalObject),
	}
	return diffDb
}

func (s *DiffStateDb) Close() {
	s.txDb.Close()
}

type LocalObject struct {
	originalCode   []byte
	currentCode    []byte
	originAccount  Account
	currentAccount Account
	originStorage  map[common.Hash]common.Hash
	currentStorage map[common.Hash]common.Hash
}

func (s *DiffStateDb) newLocalObject(obj stateObject) *LocalObject {
	localObject := &LocalObject{
		originAccount:  obj.data,
		currentAccount: obj.data,
	}
	return localObject
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (s *DiffStateDb) Exist(addr common.Address) bool {
	if obj := s.getStateObject(addr); obj != nil {
		_, exist := s.LocalObject[addr]
		if !exist {
			s.LocalObject[addr] = s.newLocalObject(*obj)
		}
		return true
	}
	return false
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *DiffStateDb) Empty(addr common.Address) bool {
	so := s.getStateObject(addr)
	if so != nil {
		_, exist := s.LocalObject[addr]
		if !exist {
			s.LocalObject[addr] = s.newLocalObject(*so)
		}
	}
	return so == nil || so.empty()
}

func (s *DiffStateDb) CreateAccount(addr common.Address) {
	newObj, prev := s.createObject(addr)
	if prev != nil {
		newObj.setBalance(prev.data.Balance)
	}
	s.LocalObject[addr] = s.newLocalObject(*newObj)
}

func (s *DiffStateDb) SubBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
			obj.currentAccount.Balance = new(big.Int).Sub(stateObject.Balance(), amount)
			s.LocalObject[addr] = obj
		} else {
			if obj.originAccount.Balance == nil {
				obj.originAccount.Balance = stateObject.Balance()
			}
			obj.currentAccount.Balance = new(big.Int).Sub(stateObject.Balance(), amount)
		}
		stateObject.SubBalance(amount)
	}
}

func (s *DiffStateDb) AddBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
			obj.currentAccount.Balance = new(big.Int).Add(stateObject.Balance(), amount)
			s.LocalObject[addr] = obj
		} else {
			if obj.originAccount.Balance == nil {
				obj.originAccount.Balance = stateObject.Balance()
			}
			obj.currentAccount.Balance = new(big.Int).Add(stateObject.Balance(), amount)
		}
		stateObject.AddBalance(amount)
	}
}

func (s *DiffStateDb) GetBalance(addr common.Address) *big.Int {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.LocalObject[addr]
		if !exist {
			s.LocalObject[addr] = s.newLocalObject(*stateObject)
		}
		return stateObject.Balance()
	}
	return common.Big0
}

func (s *DiffStateDb) GetNonce(addr common.Address) uint64 {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.LocalObject[addr]
		if !exist {
			s.LocalObject[addr] = s.newLocalObject(*stateObject)
		}
		return stateObject.Nonce()
	}
	return 0
}

func (s *DiffStateDb) SetNonce(addr common.Address, nonce uint64) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
			obj.currentAccount.Nonce = nonce
			s.LocalObject[addr] = obj
		} else {
			obj.currentAccount.Nonce = nonce
		}
		stateObject.SetNonce(nonce)
	}
}

func (s *DiffStateDb) GetCodeHash(addr common.Address) common.Hash {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return common.Hash{}
	}
	_, exist := s.LocalObject[addr]
	if !exist {
		s.LocalObject[addr] = s.newLocalObject(*stateObject)
	}
	return common.BytesToHash(stateObject.CodeHash())
}

func (s *DiffStateDb) GetCode(addr common.Address) []byte {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		code := stateObject.Code(s.db)
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
			obj.originalCode = code
		} else {
			if obj.originalCode == nil {
				obj.originalCode = code
			}
		}
		obj.currentCode = code
		return code
	}
	return nil
}

func (s *DiffStateDb) SetCode(addr common.Address, code []byte) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
		} else {
			if obj.originalCode == nil {
				obj.originalCode = stateObject.code
			}
			obj.currentCode = code
		}
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *DiffStateDb) GetCodeSize(addr common.Address) int {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		code := stateObject.Code(s.db)
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
			obj.originalCode = code
		} else {
			if obj.originalCode == nil {
				obj.originalCode = code
			}
		}
		obj.currentCode = code
		return stateObject.CodeSize(s.db)
	}
	return 0
}

func (s *DiffStateDb) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		committedState := stateObject.GetCommittedState(s.db, hash)
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
			if obj.originStorage == nil {
				obj.originStorage[hash] = committedState
			}
		} else {
			if obj.originStorage == nil {
				obj.originStorage[hash] = committedState
			}
		}
		return committedState
	}
	return common.Hash{}
}

func (s *DiffStateDb) HasSuicided(addr common.Address) bool {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.LocalObject[addr]
		if !exist {
			s.LocalObject[addr] = s.newLocalObject(*stateObject)
		}
		return stateObject.suicided
	}
	return false
}

// GetStorageProof returns the StorageProof for given key
func (s *DiffStateDb) GetStorageProof(a common.Address, key common.Hash) ([][]byte, error) {
	if s.thash.String() == "0x54ad70894edf7a04c591cd4e0f36d3a5f6e91f7c686e37e6f91161668ec120c6" {
		log.Info("test")
	}
	var proof proofList
	trie := s.StorageTrie(a)
	if trie == nil {
		return proof, errors.New("storage trie for requested address does not exist")
	}
	err := trie.Prove(crypto.Keccak256(key.Bytes()), 0, &proof)
	return proof, err
}

func (s *DiffStateDb) GetState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := s.getStateObject(addr)
	state := common.Hash{}
	if stateObject != nil {
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
		}
		state := stateObject.GetState(s.db, hash)
		if obj.originStorage == nil {
			obj.originStorage = make(map[common.Hash]common.Hash)
			obj.originStorage[hash] = state
		} else {
			if _, exist := obj.originStorage[hash]; !exist {
				obj.originStorage[hash] = state
			}
		}
		if obj.currentStorage == nil {
			obj.currentStorage = make(map[common.Hash]common.Hash)
		}
		obj.currentStorage[hash] = state
		return state
	}
	return state
}

func (s *DiffStateDb) SetState(addr common.Address, key, value common.Hash) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.LocalObject[addr]
		if !exist {
			obj = s.newLocalObject(*stateObject)
		}
		if obj.originStorage == nil {
			obj.originStorage = make(map[common.Hash]common.Hash)
			if value, exist := stateObject.originStorage[key]; exist {
				obj.originStorage[key] = value
			}
		} else {
			if _, exist := obj.originStorage[key]; !exist {
				obj.originStorage[key] = value
			}
		}
		if obj.currentStorage == nil {
			obj.currentStorage = make(map[common.Hash]common.Hash)
		}
		obj.currentStorage[key] = value
		stateObject.SetState(s.db, key, value)
	}
}

func (s *DiffStateDb) Suicide(addr common.Address) bool {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return false
	}
	s.journal.append(suicideChange{
		account:     &addr,
		prev:        stateObject.suicided,
		prevbalance: new(big.Int).Set(stateObject.Balance()),
	})

	stateObject.markSuicided()
	stateObject.data.Balance = new(big.Int)
	obj, exist := s.LocalObject[addr]
	if !exist {
		obj = s.newLocalObject(*stateObject)
		obj.currentAccount.Balance = stateObject.data.Balance
	} else {
		obj.currentAccount.Balance = stateObject.data.Balance
	}
	return true
}

func (s *DiffStateDb) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= revid
	})
	if idx == len(s.validRevisions) || s.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := s.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	s.journal.revert2(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
}

func (s *DiffStateDb) Submit() {
	if s.LocalObject == nil {
		return
	}
	txStore := &TxStore{
		Height:           s.height.String(),
		From:             s.from.Hex(),
		BlockHash:        s.bhash.Hex(),
		Coinbase:         s.coinbase.Hex(),
		TimeStamp:        s.timestamp,
		TxHash:           s.thash.Hex(),
		TxIndex:          s.txIndex,
		RawTx:            common.Bytes2Hex(s.rawTx),
		StateObjectStore: nil,
	}
	log.Debug("DiffStateDb Submit begin")
	for addr, obj := range s.LocalObject {
		originAccount := AccountStore{
			Nonce:    obj.originAccount.Nonce,
			Balance:  obj.originAccount.Balance.String(),
			CodeHash: common.Bytes2Hex(obj.originAccount.CodeHash),
		}
		currentAccount := AccountStore{
			Nonce:    obj.currentAccount.Nonce,
			Balance:  obj.currentAccount.Balance.String(),
			CodeHash: common.Bytes2Hex(obj.currentAccount.CodeHash),
		}

		originStorage := make([]storage, 0)
		for key, value := range obj.originStorage {
			store := storage{
				Key:   key.Hex(),
				Value: value.Hex(),
			}
			originStorage = append(originStorage, store)
		}

		currentStorage := make([]storage, 0)
		for key, value := range obj.currentStorage {
			store := storage{
				Key:   key.Hex(),
				Value: value.Hex(),
			}
			currentStorage = append(currentStorage, store)
		}
		stateObj := stateObjectStore{
			Address:        addr.Hex(),
			OriginalCode:   common.Bytes2Hex(obj.originalCode),
			CurrentCode:    common.Bytes2Hex(obj.currentCode),
			OriginAccount:  originAccount,
			CurrentAccount: currentAccount,
			OriginStorage:  originStorage,
			CurrentStorage: currentStorage,
		}
		txStore.StateObjectStore = append(txStore.StateObjectStore, stateObj)
	}
	txStoreBytes, err := json.Marshal(txStore)
	if err != nil {
		panic("cannot marshal txStore")
	}
	log.Debug("Submit", "txStore", string(txStoreBytes))
	if s.txDb != nil {
		err = s.txDb.InsertTx(s.thash.Hex(), string(txStoreBytes))
		if err != nil {
			log.Warn(fmt.Sprintf("cannot InsertTx %v err %v", s.thash.Hex(), err))
		}
	} else {
		log.Warn("Ignore tx", "tx message", string(txStoreBytes))
	}
	s.LocalObject = make(map[common.Address]*LocalObject)
	log.Debug("DiffStateDb Submit end")
}

type TxStore struct {
	Height           string             `json:"height"`
	From             string             `json:"from"`
	BlockHash        string             `json:"blockHash"`
	Coinbase         string             `json:"coinbase"`
	TimeStamp        uint64             `json:"timeStamp"`
	TxHash           string             `json:"txHash"`
	TxIndex          int                `json:"txIndex"`
	RawTx            string             `json:"rawTx"`
	StateObjectStore []stateObjectStore `json:"stateObjectStore"`
}

type AccountStore struct {
	Nonce    uint64 `json:"nonce"`
	Balance  string `json:"balance"`
	CodeHash string `json:"codeHash"`
}

type storage struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type stateObjectStore struct {
	Address        string       `json:"address"`
	OriginalCode   string       `json:"originalCode"`
	CurrentCode    string       `json:"currentCode"`
	OriginAccount  AccountStore `json:"originAccount"`
	CurrentAccount AccountStore `json:"currentAccount"`
	OriginStorage  []storage    `json:"originStorage"`
	CurrentStorage []storage    `json:"currentStorage"`
}
