package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
	fastssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	ethpb "github.com/prysmaticlabs/prysm/proto/eth/v1alpha1"

	"google.golang.org/protobuf/proto"
)

func main() {
	dirname, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	// dbPath := "db/kw-beacon.db" // empty local one
	dbPath := dirname + "/.eth2/beaconchaindata/beaconchain.db" // prysm db location

	// Open the my.db data file in a nearby directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		print("\nfailed to open db")
		log.Fatal(err)
	}
	defer db.Close()
	exitCh := make(chan bool)
	//readCh := make(chan string)

	go monitorBoltStats(db, exitCh)
	// go setupDb(db, readCh)
	// insertSomeData(db)
	// <-readCh // use the readCh to block and wait until ready

	//listBuckets(db)
	readBeaconData(db)
	//readSomeData(db)

	done := <-exitCh
	fmt.Println("Hello, World!", done)
}

func decode(ctx context.Context, data []byte, dst proto.Message) error {
	//ctx, span := trace.StartSpan(ctx, "BeaconDB.decode")
	//defer span.End()

	data, err := snappy.Decode(nil, data)
	if err != nil {
		return err
	}
	if isSSZStorageFormat(dst) {
		return dst.(fastssz.Unmarshaler).UnmarshalSSZ(data)
	}
	return proto.Unmarshal(data, dst)
}

func readBeaconData(db *bolt.DB) {
	db.View(func(tx *bolt.Tx) error {
		print("\n reading beacon data\n")
		b := tx.Bucket([]byte("blocks"))
		c := b.Cursor()

		counter := 1
		for k, v := c.First(); k != nil && counter < 3; k, v = c.Next() {
			counter += 1
			block := &ethpb.SignedBeaconBlock{}

			data, err := snappy.Decode(nil, v)
			if err != nil {
				print("\n\ndecode error")
				return err
			}

			var ctx context.Context
			if err := decode(ctx, v, block); err != nil {
				print("\ndecode error")
			}
			print("\nUnmarshaled block: ", block, "\nEncoded block: ", data, "\n")

			// test := proto.Unmarshal(data, block)
			//fmt.Printf("\n\nRaw blockType and data: %v, %T", block, v)
			//test := block.UnmarshalSSZ(data)
			//test := block.(fastssz.Unmarshaler).UnmarshalSSZ(data)
			//print("\nUnmarshaled block: ", test, "\nEncoded block: ", data)

			//var val map[string]interface{}
			//json.Unmarshal(test, val)
			//fmt.Println("read a row in beacon data")
			//fmt.Printf("read row in beacon data: key=%v, value=%v\n", k, val)
		}

		return nil
	})
}

func readSomeData(db *bolt.DB) {
	// read
	db.View(func(tx *bolt.Tx) error {
		print("\ndoing view\n")
		b := tx.Bucket([]byte("kwbkt"))

		b.ForEach(func(k, v []byte) error {
			decodedKey := binary.BigEndian.Uint64(k)
			fmt.Printf("record in bolt: key(%v), val(%s), test(%x)\n", decodedKey, v, k)
			return nil
		})

		return nil
	})
}

func insertSomeData(db *bolt.DB) {
	print("\ninsert data called")
	db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("kwbkt"))
		var err error

		for i := 0; i < 15; i++ {
			// This returns an error only if the Tx is closed or not writeable.
			// That can't happen in an Update() call so I ignore the error check.
			id, _ := b.NextSequence()
			binId := make([]byte, 8)
			print("\nnew id ", id, " bin ", binId, "\n")
			binary.BigEndian.PutUint64(binId, id)

			val, _ := json.Marshal(map[string]interface{}{
				"internalDataStuff": time.Now(),
			})
			err = b.Put(binId, val)
		}

		return err
	})
}

// Buckets prints a list of all buckets.
func listBuckets(db *bolt.DB) {
	fmt.Println("Buckets...")
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			fmt.Println(string(name))
			return nil
		})
	})
	if err != nil {
		fmt.Println(err)
		return
	}
}

func setupDb(db *bolt.DB, ch chan string) (interface{}, error) {
	fmt.Println("test fn")

	if err := db.Update(func(tx *bolt.Tx) error {
		return createBuckets(
			tx,
			attestationsBucket,
			blocksBucket,
			stateBucket,
			proposerSlashingsBucket,
			attesterSlashingsBucket,
			voluntaryExitsBucket,
			chainMetadataBucket,
			checkpointBucket,
			powchainBucket,
			stateSummaryBucket,
			// Indices buckets.
			attestationHeadBlockRootBucket,
			attestationSourceRootIndicesBucket,
			attestationSourceEpochIndicesBucket,
			attestationTargetRootIndicesBucket,
			attestationTargetEpochIndicesBucket,
			blockSlotIndicesBucket,
			stateSlotIndicesBucket,
			blockParentRootIndicesBucket,
			finalizedBlockRootsIndexBucket,
			// State management service bucket.
			newStateServiceCompatibleBucket,
			// Migrations
			migrationsBucket,
		)
	}); err != nil {
		ch <- "setup hit error"
		return nil, err
	}
	// listBuckets(db)
	ch <- "setup done"
	return true, nil
}

func createBuckets(tx *bolt.Tx, buckets ...[]byte) error {
	for _, bucket := range buckets {
		if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
			return err
		}
	}
	if _, err := tx.CreateBucketIfNotExists([]byte("kwbkt")); err != nil {
		return err
	}
	return nil
}

func monitorBoltStats(db *bolt.DB, exitCh chan bool) {
	fmt.Println("\nmonitoring bolt stats..")
	// Grab the initial stats.
	prev := db.Stats()

	count := 0
	for {
		count += 1
		if count > 2 {
			exitCh <- true
		}

		// Wait for 10s.
		time.Sleep(2 * time.Second)

		// Grab the current stats and diff them.
		stats := db.Stats()
		diff := stats.Sub(&prev)

		// Encode stats to JSON and print to STDERR.
		fmt.Println("\nstats:")
		json.NewEncoder(os.Stderr).Encode(diff)

		// Save stats for the next loop.
		prev = stats
	}
}

// isSSZStorageFormat returns true if the object type should be saved in SSZ encoded format.
func isSSZStorageFormat(obj interface{}) bool {
	switch obj.(type) {
	//case *pb.BeaconState:
	//	return true
	case *ethpb.SignedBeaconBlock:
		return true
	case *ethpb.SignedAggregateAttestationAndProof:
		return true
	case *ethpb.BeaconBlock:
		return true
	case *ethpb.Attestation:
		return true
	case *ethpb.Deposit:
		return true
	case *ethpb.AttesterSlashing:
		return true
	case *ethpb.ProposerSlashing:
		return true
	case *ethpb.VoluntaryExit:
		return true
	default:
		return false
	}
}

// schema from prysm
var (
	attestationsBucket      = []byte("attestations")
	blocksBucket            = []byte("blocks")
	stateBucket             = []byte("state")
	stateSummaryBucket      = []byte("state-summary")
	proposerSlashingsBucket = []byte("proposer-slashings")
	attesterSlashingsBucket = []byte("attester-slashings")
	voluntaryExitsBucket    = []byte("voluntary-exits")
	chainMetadataBucket     = []byte("chain-metadata")
	checkpointBucket        = []byte("check-point")
	powchainBucket          = []byte("powchain")

	// Deprecated: This bucket was migrated in PR 6461. Do not use, except for migrations.
	slotsHasObjectBucket = []byte("slots-has-objects")
	// Deprecated: This bucket was migrated in PR 6461. Do not use, except for migrations.
	archivedRootBucket = []byte("archived-index-root")

	// Key indices buckets.
	blockParentRootIndicesBucket        = []byte("block-parent-root-indices")
	blockSlotIndicesBucket              = []byte("block-slot-indices")
	stateSlotIndicesBucket              = []byte("state-slot-indices")
	attestationHeadBlockRootBucket      = []byte("attestation-head-block-root-indices")
	attestationSourceRootIndicesBucket  = []byte("attestation-source-root-indices")
	attestationSourceEpochIndicesBucket = []byte("attestation-source-epoch-indices")
	attestationTargetRootIndicesBucket  = []byte("attestation-target-root-indices")
	attestationTargetEpochIndicesBucket = []byte("attestation-target-epoch-indices")
	finalizedBlockRootsIndexBucket      = []byte("finalized-block-roots-index")

	// Specific item keys.
	headBlockRootKey          = []byte("head-root")
	genesisBlockRootKey       = []byte("genesis-root")
	depositContractAddressKey = []byte("deposit-contract")
	justifiedCheckpointKey    = []byte("justified-checkpoint")
	finalizedCheckpointKey    = []byte("finalized-checkpoint")
	powchainDataKey           = []byte("powchain-data")

	// Deprecated: This index key was migrated in PR 6461. Do not use, except for migrations.
	lastArchivedIndexKey = []byte("last-archived")
	// Deprecated: This index key was migrated in PR 6461. Do not use, except for migrations.
	savedStateSlotsKey = []byte("saved-state-slots")

	// New state management service compatibility bucket.
	newStateServiceCompatibleBucket = []byte("new-state-compatible")

	// Migrations
	migrationsBucket = []byte("migrations")
)
