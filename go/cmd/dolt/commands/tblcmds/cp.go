package tblcmds

import (
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/env"
	"os"
)

var tblCpShortDesc = "Makes a copy of a table"
var tblCpLongDesc = "The dolt table cp command will make a copy of a table at a given commit.  If a commit is not specified " +
	"the copy is made of the current working table.\n" +
	"\n" +
	"If a table exists at the target location this command will fail unless the <b>--force|-f</b> flag is provided.  In this case " +
	"the table at the target location will be overwritten with the copied table.\n" +
	"\n" +
	"All changes will be applied to the working tables and will need to be staged using <b>dolt add</b> and committed " +
	"using <b>dolt commit</b>"

var tblCpSynopsis = []string{
	"[-f] [<commit>] [--] <oldtable> <newtable>",
}

func Cp(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["commit"] = "The state at which point the table whill be copied."
	ap.ArgListHelp["oldtable"] = "The table being copied."
	ap.ArgListHelp["newtable"] = "The destination where the table is being copied to."
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the Force flag will allow the target to be overwritten.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblCpShortDesc, tblCpLongDesc, tblCpSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() < 2 || apr.NArg() > 3 {
		fmt.Fprintln(os.Stderr, "invalid usage")
		usage()
		return 1
	}

	force := apr.Contains(forceParam)
	working, verr := commands.GetWorkingWithVErr(dEnv)
	root := working

	if verr == nil {
		var old, new string
		if apr.NArg() == 3 {
			var cm *doltdb.Commit
			cm, verr = commands.ResolveCommitWithVErr(dEnv, apr.Arg(0), dEnv.RepoState.Branch)
			if verr == nil {
				root = cm.GetRootValue()
			}

			old, new = apr.Arg(1), apr.Arg(2)
		} else {
			old, new = apr.Arg(0), apr.Arg(1)
		}

		if verr == nil {
			tbl, ok := root.GetTable(old)
			if ok {
				if !force && working.HasTable(new) {
					verr = errhand.BuildDError("Data already exists in '%s'.  Use -f to overwrite.", new).Build()
				} else {
					working = working.PutTable(dEnv.DoltDB, new, tbl)
					verr = commands.UpdateWorkingWithVErr(dEnv, working)
				}
			} else {
				verr = errhand.BuildDError("Table '%s' not found in root %s", old, root.HashOf().String()).Build()
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}