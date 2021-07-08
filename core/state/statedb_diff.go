package state

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
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
	txDb        TxDB
	rawTx       []byte
	height      *big.Int
	coinbase    common.Address
	timestamp   uint64
	from        common.Address
	localObject map[common.Address]*LocalObject
}

func NewWithTxDb(root common.Hash, db Database, snaps *snapshot.Tree, txDb TxDB) (*DiffStateDb, error) {
	stateDb, err := New(root, db, snaps)
	if err != nil {
		return nil, err
	}

	diffDb := &DiffStateDb{
		StateDB:     stateDb,
		txDb:        txDb,
		localObject: make(map[common.Address]*LocalObject),
	}
	return diffDb, nil
}

type LocalObject struct {
	originCode     []byte
	currentCode    []byte
	originAccount  Account
	currentAccount Account
	originStorage  map[common.Hash]common.Hash
	currentStorage map[common.Hash]common.Hash
}

func newLocalObject(obj stateObject) *LocalObject {
	return &LocalObject{
		originCode:     obj.code,
		currentCode:    obj.code,
		originAccount:  obj.data,
		currentAccount: obj.data,
		originStorage:  obj.originStorage,
		currentStorage: obj.originStorage,
	}
}

func (s *DiffStateDb) Prepare(height *big.Int, coinbase common.Address, thash, bhash common.Hash, time uint64, ti int, rawTx []byte, from common.Address) {
	if thash.String() == "0xf904d12085b5c8dd5cf6af7cf98a34f5673f5a22abb14a17fd5ec5ab5008a802" {
		log.Info(s.thash.String())
	}
	s.localObject = make(map[common.Address]*LocalObject)
	s.height = height
	s.coinbase = coinbase
	s.timestamp = time
	s.thash = thash
	s.bhash = bhash
	s.txIndex = ti
	s.rawTx = rawTx
	s.from = from
	s.accessList = newAccessList()
}

func (s *DiffStateDb) CreateAccount(addr common.Address) {
	newObj, prev := s.createObject(addr)
	if prev != nil {
		newObj.setBalance(prev.data.Balance)
	}
	s.localObject[addr] = newLocalObject(*newObj)
}

func obj2LocalObj(obj stateObject) LocalObject {
	return LocalObject{
		originCode:     nil,
		currentCode:    nil,
		originAccount:  Account{},
		currentAccount: Account{},
		originStorage:  nil,
		currentStorage: nil,
	}
}

func (s *DiffStateDb) SubBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		_, exist := s.localObject[addr]
		if !exist {
			s.localObject[addr] = newLocalObject(*stateObject)
		}
		stateObject.SubBalance(amount)
	}
}

// GetOrNewStateObject retrieves a state object or create a new state object if nil.
func (s *DiffStateDb) GetOrNewStateObject(addr common.Address) *stateObject {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		stateObject, _ = s.createObject(addr)
	}
	_, exist := s.localObject[addr]
	if !exist {
		s.localObject[addr] = newLocalObject(*stateObject)
	}
	return stateObject
}

func (s *DiffStateDb) AddBalance(addr common.Address, amount *big.Int) {
	if addr.String() == "0xA8e8F14732658E4B51E8711931053a8A69BaF2B1" {
		log.Info("AddBalance")
	}
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.localObject[addr]
		if !exist {
			obj = newLocalObject(*stateObject)
			obj.currentAccount.Balance = new(big.Int).Add(stateObject.Balance(), amount)
			s.localObject[addr] = obj
		} else {
			obj.currentAccount.Balance = new(big.Int).Add(stateObject.Balance(), amount)
		}
		stateObject.AddBalance(amount)
	}
}

func (s *DiffStateDb) GetBalance(addr common.Address) *big.Int {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.localObject[addr]
		if !exist {
			s.localObject[addr] = newLocalObject(*stateObject)
		}
		return stateObject.Balance()
	}
	return common.Big0
}

func (s *DiffStateDb) GetNonce(addr common.Address) uint64 {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.localObject[addr]
		if !exist {
			s.localObject[addr] = newLocalObject(*stateObject)
		}
		return stateObject.Nonce()
	}
	return 0
}

func (s *DiffStateDb) SetNonce(addr common.Address, nonce uint64) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.localObject[addr]
		if !exist {
			obj = newLocalObject(*stateObject)
			obj.currentAccount.Nonce = nonce
			s.localObject[addr] = obj
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
	_, exist := s.localObject[addr]
	if !exist {
		s.localObject[addr] = newLocalObject(*stateObject)
	}
	return common.BytesToHash(stateObject.CodeHash())
}

func (s *DiffStateDb) GetCode(addr common.Address) []byte {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		obj, exist := s.localObject[addr]
		if !exist {
			s.localObject[addr] = newLocalObject(*stateObject)
		} else {
			obj.currentCode = stateObject.code
		}
		return stateObject.Code(s.db)
	}
	return nil
}

func (s *DiffStateDb) SetCode(addr common.Address, code []byte) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		obj, exist := s.localObject[addr]
		if !exist {
			obj = newLocalObject(*stateObject)
			s.localObject[addr] = obj
		} else {
			obj.currentCode = code
		}
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *DiffStateDb) GetCodeSize(addr common.Address) int {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.localObject[addr]
		if !exist {
			s.localObject[addr] = newLocalObject(*stateObject)
		}
		return stateObject.CodeSize(s.db)
	}
	return 0
}

func (s *DiffStateDb) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.localObject[addr]
		if !exist {
			s.localObject[addr] = newLocalObject(*stateObject)
		}
		return stateObject.GetCommittedState(s.db, hash)
	}
	return common.Hash{}
}

func (s *DiffStateDb) GetState(addr common.Address, hash common.Hash) common.Hash {
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		_, exist := s.localObject[addr]
		if !exist {
			s.localObject[addr] = newLocalObject(*stateObject)
		}
		return stateObject.GetState(s.db, hash)
	}
	return common.Hash{}
}

func (s *DiffStateDb) SetState(addr common.Address, key, value common.Hash) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
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
	return true
}

func (s *DiffStateDb) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
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
	if s.localObject == nil {
		return
	}
	log.Info("DiffStateDb Submit begin")
	for addr, obj := range s.localObject {
		log.Info("DiffStateDb Submit txHash", "txHash", s.thash.String())
		log.Info("DiffStateDb Submit address", "txHash", addr.String())
		log.Info("DiffStateDb Submit account info", "originCode", common.Bytes2Hex(obj.originCode), "currentCode", common.Bytes2Hex(obj.currentCode),
			"originAccount Nonce", obj.originAccount.Nonce, "originAccount.Balance", obj.originAccount.Balance.String(), "originAccount.CodeHash", common.Bytes2Hex(obj.originAccount.CodeHash),
			"currentAccount Nonce", obj.currentAccount.Nonce, "currentAccount.Balance", obj.currentAccount.Balance.String(), "currentAccount.CodeHash", common.Bytes2Hex(obj.currentAccount.CodeHash),
		)
		for key, value := range obj.originStorage {
			log.Info("DiffStateDb Submit storage", "key", key.String(), "value", value.String())
		}
	}
	log.Info("DiffStateDb Submit end")
}

//type LocalObject struct {
//	originCode     []byte
//	currentCode    []byte
//	originAccount  Account
//	currentAccount Account
//	originStorage  map[common.Hash]common.Hash
//	currentStorage map[common.Hash]common.Hash
//}

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
	OriginCode     string       `json:"originCode"`
	CurrentCode    string       `json:"currentCode"`
	OriginAccount  AccountStore `json:"originAccount"`
	CurrentAccount AccountStore `json:"currentAccount"`
	OriginStorage  []storage    `json:"originStorage"`
	CurrentStorage []storage    `json:"currentStorage"`
	Deleted        bool         `json:"deleted"`
}
