package actions

import (
	"errors"
	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/noms/go/util/math"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/set"
)

var ErrAlreadyExists = errors.New("already Exists")
var ErrCOBranchDelete = errors.New("attempted to delete checked out branch")

func MoveBranch(dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	err := CopyBranch(dEnv, oldBranch, newBranch, force)

	if err != nil {
		return err
	}

	if dEnv.RepoState.Branch == oldBranch {
		dEnv.RepoState.Branch = newBranch
		err = dEnv.RepoState.Save()

		if err != nil {
			return err
		}
	}

	return DeleteBranch(dEnv, oldBranch, true)
}

func CopyBranch(dEnv *env.DoltEnv, oldBranch, newBranch string, force bool) error {
	if !dEnv.DoltDB.HasBranch(oldBranch) {
		return doltdb.ErrBranchNotFound
	} else if !force && dEnv.DoltDB.HasBranch(newBranch) {
		return ErrAlreadyExists
	} else if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, _ := doltdb.NewCommitSpec("head", oldBranch)
	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return err
	}

	return dEnv.DoltDB.NewBranchAtCommit(newBranch, cm)
}

func DeleteBranch(dEnv *env.DoltEnv, brName string, force bool) error {
	if !dEnv.DoltDB.HasBranch(brName) {
		return doltdb.ErrBranchNotFound
	} else if dEnv.RepoState.Branch == brName {
		return ErrCOBranchDelete
	}

	return dEnv.DoltDB.DeleteBranch(brName)
}

func CreateBranch(dEnv *env.DoltEnv, newBranch, startingPoint string) error {
	if dEnv.DoltDB.HasBranch(newBranch) {
		return ErrAlreadyExists
	}

	if !doltdb.IsValidUserBranchName(newBranch) {
		return doltdb.ErrInvBranchName
	}

	cs, err := doltdb.NewCommitSpec(startingPoint, dEnv.RepoState.Branch)

	if err != nil {
		return err
	}

	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return err
	}

	return dEnv.DoltDB.NewBranchAtCommit(newBranch, cm)
}

func CheckoutBranch(dEnv *env.DoltEnv, brName string) error {
	if !dEnv.DoltDB.HasBranch(brName) {
		return doltdb.ErrBranchNotFound
	}

	currRoots, err := getRoots(dEnv, HeadRoot, WorkingRoot, StagedRoot)

	if err != nil {
		return err
	}

	cs, err := doltdb.NewCommitSpec("head", brName)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	cm, err := dEnv.DoltDB.Resolve(cs)

	if err != nil {
		return RootValueUnreadable{HeadRoot, err}
	}

	newRoot := cm.GetRootValue()
	conflicts := set.NewStrSet([]string{})
	wrkTblHashes := tblHashesForCO(currRoots[HeadRoot], newRoot, currRoots[WorkingRoot], conflicts)
	stgTblHashes := tblHashesForCO(currRoots[HeadRoot], newRoot, currRoots[StagedRoot], conflicts)

	if conflicts.Size() > 0 {
		return CheckoutWouldOverwrite{conflicts.AsSlice()}
	}

	wrkHash, err := writeRoot(dEnv, wrkTblHashes)

	if err != nil {
		return err
	}

	stgHash, err := writeRoot(dEnv, stgTblHashes)

	if err != nil {
		return err
	}

	dEnv.RepoState.Branch = brName
	dEnv.RepoState.Working = wrkHash.String()
	dEnv.RepoState.Staged = stgHash.String()
	dEnv.RepoState.Save()

	return nil
}

var emptyHash = hash.Hash{}

func tblHashesForCO(oldRoot, newRoot, changedRoot *doltdb.RootValue, conflicts *set.StrSet) map[string]hash.Hash {
	resultMap := make(map[string]hash.Hash)
	for _, tblName := range newRoot.GetTableNames() {
		oldHash, _ := oldRoot.GetTableHash(tblName)
		newHash, _ := newRoot.GetTableHash(tblName)
		changedHash, _ := changedRoot.GetTableHash(tblName)

		if oldHash == changedHash {
			resultMap[tblName] = newHash
		} else if oldHash == newHash {
			resultMap[tblName] = changedHash
		} else if newHash == changedHash {
			resultMap[tblName] = oldHash
		} else {
			conflicts.Add(tblName)
		}
	}

	for _, tblName := range changedRoot.GetTableNames() {
		if _, exists := resultMap[tblName]; !exists {
			oldHash, _ := oldRoot.GetTableHash(tblName)
			changedHash, _ := changedRoot.GetTableHash(tblName)

			if oldHash == emptyHash {
				resultMap[tblName] = changedHash
			} else if oldHash != changedHash {
				conflicts.Add(tblName)
			}
		}
	}

	return resultMap
}

func writeRoot(dEnv *env.DoltEnv, tblHashes map[string]hash.Hash) (hash.Hash, error) {
	for k, v := range tblHashes {
		if v == emptyHash {
			delete(tblHashes, k)
		}
	}

	root, err := doltdb.NewRootValue(dEnv.DoltDB.ValueReadWriter(), tblHashes)

	if err != nil {
		if err == doltdb.ErrHashNotFound {
			return emptyHash, errors.New("corrupted database? Can't find hash of current table")
		} else {
			return emptyHash, env.ErrNomsIO
		}
	}

	return dEnv.DoltDB.WriteRootValue(root)
}

func getDifferingTables(root1, root2 *doltdb.RootValue) []string {
	tbls := root1.GetTableNames()
	differing := make([]string, 0, len(tbls))
	for _, tbl := range tbls {
		hsh1, _ := root1.GetTableHash(tbl)
		hsh2, _ := root2.GetTableHash(tbl)

		if hsh1 != hsh2 {
			differing = append(differing, tbl)
		}
	}

	return differing
}

func intersect(sl1, sl2 []string) []string {
	sl1Members := make(map[string]struct{})

	for _, mem := range sl1 {
		sl1Members[mem] = struct{}{}
	}

	maxIntSize := math.MaxInt(len(sl1), len(sl2))

	intersection := make([]string, 0, maxIntSize)
	for _, mem := range sl2 {
		if _, ok := sl1Members[mem]; ok {
			intersection = append(intersection, mem)
		}
	}

	return intersection
}

func BranchOrTable(dEnv *env.DoltEnv, str string) (isBranch bool, rootsWithTbl RootTypeSet, err error) {
	roots, err := getRoots(dEnv, ActiveRoots...)

	if err != nil {
		return false, nil, err
	}

	rootsWithBranch := make([]RootType, 0, len(roots))
	for rt, root := range roots {
		if root.HasTable(str) {
			rootsWithBranch = append(rootsWithBranch, rt)
		}
	}

	return dEnv.DoltDB.HasBranch(str), NewRootTypeSet(rootsWithBranch...), nil
}