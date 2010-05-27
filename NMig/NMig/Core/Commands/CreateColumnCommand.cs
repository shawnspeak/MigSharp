﻿using System.Collections.Generic;
using System.Data;

namespace NMig.Core.Commands
{
    internal class CreateColumnCommand : ICommand
    {
        private readonly string _columnName;
        private readonly DbType _type;
        private readonly bool _isPrimaryKey;

        public string ColumnName { get { return _columnName; } }
        public DbType Type { get { return _type; } }
        public bool IsPrimaryKey { get { return _isPrimaryKey; } }

        public CreateColumnCommand(string columnName, DbType type, bool isPrimaryKey)
        {
            _columnName = columnName;
            _type = type;
            _isPrimaryKey = isPrimaryKey;
        }

        public IEnumerable<ICommand> Children { get { yield break; } }
    }
}