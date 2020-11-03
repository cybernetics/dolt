// Copyright 2020 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package enginetest

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

type harnessIndexDriver struct {
	dbs []sqle.Database
}

var _ sql.IndexDriver = (*harnessIndexDriver)(nil)

func NewIndexDriver(sqlDbs []sql.Database) sql.IndexDriver {
	var dbs []sqle.Database
	for _, sqlDb := range sqlDbs {
		db, ok := sqlDb.(sqle.Database)
		if !ok { // ignore internal dbs like information_schema
			continue
		}
		dbs = append(dbs, db)
	}
	return &harnessIndexDriver{dbs}
}

func (d *harnessIndexDriver) ID() string {
	return "DoltIndexDriver"
}

func (d *harnessIndexDriver) LoadAll(ctx *sql.Context, dbName, tableName string) ([]sql.DriverIndex, error) {
	dbName = strings.ToLower(dbName)
	for _, db := range d.dbs {
		if strings.ToLower(db.Name()) != dbName {
			continue
		}
		tbl, exists, err := db.GetTableInsensitive(ctx, tableName)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, nil
		}
		indexableTable, ok := tbl.(sql.IndexedTable)
		if !ok {
			return nil, nil
		}
		indexes, err := indexableTable.GetIndexes(ctx)
		if err != nil {
			return nil, err
		}
		var driverIndexes []sql.DriverIndex
		for _, index := range indexes {
			driverIndex, ok := index.(sql.DriverIndex)
			if ok {
				driverIndexes = append(driverIndexes, driverIndex)
			}
		}
		return driverIndexes, nil
	}
	return nil, nil
}

func (d *harnessIndexDriver) Create(db, table, id string, expressions []sql.Expression, config map[string]string) (sql.DriverIndex, error) {
	panic("not implemented")
}

func (d *harnessIndexDriver) Save(*sql.Context, sql.DriverIndex, sql.PartitionIndexKeyValueIter) error {
	panic("not implemented")
}

func (d *harnessIndexDriver) Delete(sql.DriverIndex, sql.PartitionIter) error {
	panic("not implemented")
}
