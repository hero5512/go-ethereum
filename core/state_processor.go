// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"os"
	"path"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts types.Receipts
		usedGas  = new(uint64)
		header   = block.Header()
		allLogs  []*types.Log
		gp       = new(GasPool).AddGas(block.GasLimit())
	)
	preBlock := p.bc.GetBlockByNumber(block.NumberU64() - 1)
	preBlockHash := common.Hash{}
	if preBlock != nil {
		preBlockHash = preBlock.Hash()
	}
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	//vmenv := evm.NewEVM(blockContext, evm.TxContext{}, db, config, evm.Config{Debug: true, Tracer: evm.NewJSONLogger(nil, os.Stdout)})
	//vmConfig = vm.Config{
	//	EnablePreimageRecording: config.EnablePreimageRecording,
	//	EWASMInterpreter:        config.EWASMInterpreter,
	//	EVMInterpreter:          config.EVMInterpreter,
	//}
	blockContext := NewEVMBlockContext(header, p.bc, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
	var f *os.File
	defer f.Close()
	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		if tx.Hash().Hex() == "0xb55eefac0bf78c13410c84cca882fcef959e69bf6cf620bfbf63e702602666dd" || tx.Hash().Hex() == "0x1e7092e7a115c33793f90ccf960c51cf8c491917dc9caeabd6a1386d3513efbe" ||
			tx.Hash().Hex() == "0xd319782c3229a4705e6adfdf0d34447b336252a0bc8ab2b2ff0654a2dd694ff8" || tx.Hash().Hex() == "0xad876d02f6dcb4c497a3741f743bcaaf815d7f75c7e03f1d0e27a4fb9e8bfc91" ||
			tx.Hash().Hex() == "0x84bfa188422f82ea2c77b9d2da0dae9875b33ddd127ee1ed6510795068b95f13" {
			traceFile, err := os.Create(path.Join(".", fmt.Sprintf("trace-%d.json", tx.Hash().Hex())))
			if err != nil {
				panic(fmt.Sprintf("failed creating trace-file: %v", err))
			}
			cfg := vm.Config{
				EnablePreimageRecording: cfg.EnablePreimageRecording,
				EWASMInterpreter:        cfg.EWASMInterpreter,
				EVMInterpreter:          cfg.EVMInterpreter,
				Debug:                   true,
				Tracer:                  vm.NewJSONLogger(nil, traceFile),
			}
			vmenv = vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
		}
		msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number))
		if err != nil {
			return nil, nil, 0, err
		}
		txBuffer := new(bytes.Buffer)
		err = tx.EncodeRLP(txBuffer)
		if err != nil {
			log.Error("Process", "err", err)
		}
		statedb.Prepare(block.Number(), block.Coinbase(), tx.Hash(), block.Hash(), block.Time(), i, txBuffer.Bytes(), msg.From(), block.GasLimit(), block.Difficulty(), preBlockHash)
		receipt, err := applyTransaction(msg, p.config, p.bc, nil, gp, statedb, header, tx, usedGas, vmenv)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())

	return receipts, allLogs, *usedGas, nil
}

func checkFileIsExist(filename string) bool {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return false
	}
	return true
}

func applyTransaction(msg types.Message, config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, error) {
	diffDb := state.NewDiffDb(statedb)
	// Create a new context to be used in the EVM environment
	txContext := NewEVMTxContext(msg)
	// Add addresses to access list if applicable
	if config.IsYoloV2(header.Number) {
		diffDb.AddAddressToAccessList(msg.From())
		if dst := msg.To(); dst != nil {
			diffDb.AddAddressToAccessList(*dst)
			// If it's a create-tx, the destination will be added inside evm.create
		}
		for _, addr := range evm.ActivePrecompiles() {
			diffDb.AddAddressToAccessList(addr)
		}
	}

	// Update the evm with the new transaction context.
	evm.Reset(txContext, diffDb)
	// Apply the transaction to the current state (included in the env)
	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}
	// Update the state with pending changes
	var root []byte
	if config.IsByzantium(header.Number) {
		diffDb.Finalise(true)
	} else {
		root = diffDb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	receipt := types.NewReceipt(root, result.Failed(), *usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = diffDb.GetLogs(tx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = diffDb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(diffDb.TxIndex())
	diffDb.Submit()
	return receipt, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, bc, author)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, config, cfg)
	return applyTransaction(msg, config, bc, author, gp, statedb, header, tx, usedGas, vmenv)
}
