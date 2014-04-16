using MigSharp;
using MigSharp.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MigSharp.Providers
{
    [ProviderExport("Informix", InvariantName, MaximumDbObjectNameLength = MaximumDbObjectNameLength, EnableAnsiQuotesCommand = "")]
    //[Supports(DbType.AnsiString, MaximumSize = 65535, CanBeUsedAsPrimaryKey = true)] // maximum size 65,535 started in MySql 5.0.3 according to http://dev.mysql.com/doc/refman/5.0/en/char.html
    //[Supports(DbType.AnsiString)] // translates to LONGTEXT without specifying the size
    [Supports(DbType.Binary)]
    [Supports(DbType.Byte, CanBeUsedAsPrimaryKey = true, CanBeUsedAsIdentity = true)]
    [Supports(DbType.Boolean, CanBeUsedAsPrimaryKey = true)]
    [Supports(DbType.DateTime, CanBeUsedAsPrimaryKey = true)]
    [Supports(DbType.Decimal, MaximumSize = 28, MaximumScale = 28, CanBeUsedAsPrimaryKey = true)] // this is a restriction of the decimal type of the CLR (see http://support.microsoft.com/kb/932288)
    [Supports(DbType.Decimal, MaximumSize = 28, CanBeUsedAsPrimaryKey = true, CanBeUsedAsIdentity = true)] // this is a restriction of the decimal type of the CLR (see http://support.microsoft.com/kb/932288)
    [Supports(DbType.Double)]
    [Supports(DbType.Guid, CanBeUsedAsPrimaryKey = true)]
    [Supports(DbType.Int16, CanBeUsedAsPrimaryKey = true, CanBeUsedAsIdentity = true)]
    [Supports(DbType.Int32, CanBeUsedAsPrimaryKey = true, CanBeUsedAsIdentity = true)]
    [Supports(DbType.Int64, CanBeUsedAsPrimaryKey = true, CanBeUsedAsIdentity = true)]
    [Supports(DbType.Single, CanBeUsedAsPrimaryKey = true, Warning = "Using DbType.Single might give you some unexpected problems because all calculations in MySQL are done with double precision.")]
    [Supports(DbType.String, MaximumSize = 65535, CanBeUsedAsPrimaryKey = true)] // maximum size 65,535 started in MySql 5.0.3 according to http://dev.mysql.com/doc/refman/5.0/en/char.html
    [Supports(DbType.String)] // translates to LONGTEXT without specifying the Size
    [Supports(DbType.Time)]
    [Supports(DbType.UInt16)]
    [Supports(DbType.UInt32)]
    [Supports(DbType.UInt64)]
    //[Supports(DbType.AnsiStringFixedLength, MaximumSize = 255, CanBeUsedAsPrimaryKey = true)] // http://dev.mysql.com/doc/refman/5.0/en/char.html
    [Supports(DbType.StringFixedLength, MaximumSize = 255, CanBeUsedAsPrimaryKey = true)] // http://dev.mysql.com/doc/refman/5.0/en/char.html

    public class InformixProvider : IProvider
    {
        public const string InvariantName = "IBM.Data.DB2";

        public const int MaximumDbObjectNameLength = 64;

        private const string Identation = "\t";

        public bool SpecifyWith { get { return true; } }
        public string Dbo { get { return "[dbo]."; } }

        protected static string Escape(string name)
        {
            return string.Format(CultureInfo.InvariantCulture, @"""{0}""", name);
        }

        protected static string ValueEscape(string name)
        {
            return string.Format(CultureInfo.InvariantCulture, @"'{0}'", name);
        }

        private static string CreateTable(string tableName)
        {
            return string.Format(CultureInfo.InvariantCulture, "CREATE TABLE {0}", Escape(tableName));
        }

        private static string AlterTable(string tableName)
        {
            return string.Format(CultureInfo.InvariantCulture, "ALTER TABLE {0}", Escape(tableName));
        }

        protected string DropConstraint(string tableName, string constraintName)
        {
            return AlterTable(tableName) + string.Format(CultureInfo.InvariantCulture, " DROP CONSTRAINT [{0}]", constraintName);
        }

        protected static string GetDefaultConstraintName(string tableName, string columnName)
        {
            return ObjectNameHelper.GetObjectName(tableName, "DF", MaximumDbObjectNameLength, columnName);
        }

        private static string PrefixIfObjectExists(string objectName, string commandTextToBePrefixed)
        {
            return string.Format(CultureInfo.InvariantCulture, "IF OBJECT_ID('{0}') IS NOT NULL ", objectName) + commandTextToBePrefixed;
        }

        private string GetDefaultValueAsString(object value)
        {
            if (value is SpecialDefaultValue)
            {
                switch ((SpecialDefaultValue)value)
                {
                    case SpecialDefaultValue.CurrentDateTime:
                        return "GETDATE()";
                    default:
                        throw new ArgumentOutOfRangeException("value");
                }
            }
            else if (value is DateTime)
            {
                return ConvertToSql(value, DbType.DateTime);
            }
            else if (value is string)
            {
                return ConvertToSql(value, DbType.String);
            }
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        protected IEnumerable<string> DropDefaultConstraint(string tableName, string columnName, bool checkIfExists)
        {
            string constraintName = GetDefaultConstraintName(tableName, columnName);
            string commandText = DropConstraint(tableName, constraintName);
            if (checkIfExists)
            {
                commandText = PrefixIfObjectExists(constraintName, commandText);
            }
            yield return commandText;
        }

        private string GetDefaultConstraintClause(string tableName, string columnName, object value)
        {
            string constraintName = GetDefaultConstraintName(tableName, columnName);
            string defaultConstraintClause = string.Empty;
            if (value != null)
            {
                string defaultValue = GetDefaultValueAsString(value);
                defaultConstraintClause = string.Format(CultureInfo.InvariantCulture, " CONSTRAINT [{0}]  DEFAULT {1}", constraintName, defaultValue);
            }
            return defaultConstraintClause;
        }

        private IEnumerable<string> AddConstraint(string tableName, string constraintName, IEnumerable<string> columnNames, string constraintType)
        {
            yield return AlterTable(tableName) + string.Format(CultureInfo.InvariantCulture, " ADD  CONSTRAINT [{0}] {3} {1}({1}\t{2}{1}){4}",
                constraintName,
                Environment.NewLine,
                string.Join("," + Environment.NewLine + "\t", columnNames.Select(Escape).ToArray()),
                constraintType,
                SpecifyWith ? "WITH (SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF)" : string.Empty);
        }

        public IEnumerable<string> AddColumn(string tableName, Column column)
        {
            // assemble ALTER TABLE statements
            string commandText = string.Format(CultureInfo.InvariantCulture, @"{0} {1}{2}ADD ", AlterTable(tableName), Environment.NewLine, Identation);
            string defaultConstraintClause = GetDefaultConstraintClause(tableName, column.Name, column.DefaultValue);
            commandText += string.Format(CultureInfo.InvariantCulture, "\"{0}\" {1} {2} {3}",
                column.Name,
                GetTypeSpecifier(column.DataType),
                defaultConstraintClause,
                column.IsNullable ? string.Empty : "NOT NULL");
            yield return commandText;
        }

        public IEnumerable<string> AddForeignKey(string tableName, string referencedTableName, IEnumerable<ColumnReference> columnNames, string constraintName)
        {
            yield return AlterTable(tableName) + string.Format(CultureInfo.InvariantCulture, "  ADD  CONSTRAINT [{0}] FOREIGN KEY({1}){2}REFERENCES {3} ({4})",
                constraintName,
                string.Join(", ", columnNames.Select(n => Escape(n.ColumnName)).ToArray()),
                Environment.NewLine,
                Escape(referencedTableName),
                string.Join(", ", columnNames.Select(n => Escape(n.ReferencedColumnName)).ToArray()));
        }

        public IEnumerable<string> AddIndex(string tableName, IEnumerable<string> columnNames, string indexName)
        {
            yield return string.Format(CultureInfo.InvariantCulture, "CREATE INDEX {0} ON {4}{1} {2}({2}\t{3}{2}){5}",
                Escape(indexName),
                Escape(tableName),
                Environment.NewLine,
                string.Join(string.Format(CultureInfo.InvariantCulture, ",{0}\t", Environment.NewLine), columnNames.Select(Escape).ToArray()),
                Dbo,
                SpecifyWith ? "WITH (SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF)" : string.Empty);
        }

        public IEnumerable<string> AddPrimaryKey(string tableName, IEnumerable<string> columnNames, string constraintName)
        {
            return AddConstraint(tableName, constraintName, columnNames, "PRIMARY KEY");
        }

        public IEnumerable<string> AddUniqueConstraint(string tableName, IEnumerable<string> columnNames, string constraintName)
        {
            return AddConstraint(tableName, constraintName, columnNames, "UNIQUE");
        }

        public IEnumerable<string> AlterColumn(string tableName, Column column)
        {
            // remove any existing default value constraints (before possibly adding new ones)
            foreach (string text in DropDefaultConstraint(tableName, column.Name, true))
            {
                yield return text;
            }
            yield return AlterTable(tableName) + string.Format(CultureInfo.InvariantCulture, " MODIFY COLUMN [{0}] {1} {2} {3}NULL",
                column.Name,
                GetTypeSpecifier(column.DataType),
                String.Empty,
                column.IsNullable ? string.Empty : "NOT ");
            if (column.DefaultValue != null)
            {
                yield return AlterTable(tableName) + string.Format(CultureInfo.InvariantCulture, " ADD {0} FOR {1}",
                    GetDefaultConstraintClause(tableName, column.Name, column.DefaultValue),
                    Escape(column.Name));
            }
        }

        public string ConvertToSql(object value, DbType targetDbType)
        {
            return SqlScriptingHelper.ToSql(value, targetDbType);
        }

        public IEnumerable<string> CreateTable(string tableName, IEnumerable<CreatedColumn> columns, string primaryKeyConstraintName)
        {
            string commandText = string.Empty;
            List<string> primaryKeyColumns = new List<string>();
            commandText += string.Format(CultureInfo.InvariantCulture, @"{0}({1}", CreateTable(tableName), Environment.NewLine);
            bool columnDelimiterIsNeeded = false;
            foreach (CreatedColumn column in columns)
            {
                if (columnDelimiterIsNeeded) commandText += string.Format(CultureInfo.InvariantCulture, ",{0}", Environment.NewLine);

                if (column.IsPrimaryKey)
                {
                    primaryKeyColumns.Add(column.Name);
                }

                string defaultConstraintClause = GetDefaultConstraintClause(tableName, column.Name, column.DefaultValue);
                commandText += string.Format(CultureInfo.InvariantCulture, "{0}{1} {2} {3} {4}",
                    Identation,
                    Escape(column.Name),
                    GetTypeSpecifier(column.DataType),
                    defaultConstraintClause,
                    column.IsNullable ? string.Empty : "NOT NULL"
                    );

                columnDelimiterIsNeeded = true;
            }

            if (primaryKeyColumns.Count > 0)
            {
                // FEATURE: support clustering
                commandText += string.Format(CultureInfo.InvariantCulture, ",{0} primary key (",
                    Environment.NewLine);                

                columnDelimiterIsNeeded = false;
                foreach (string column in primaryKeyColumns)
                {
                    if (columnDelimiterIsNeeded) 
                        commandText += ", ";

                    // FEATURE: make sort order configurable
                    commandText += string.Format(CultureInfo.InvariantCulture, "{0}", Escape(column));

                    columnDelimiterIsNeeded = true;
                }
                commandText += ")";
            }

            foreach (var uniqueColumns in columns
                .Where(c => !string.IsNullOrEmpty(c.UniqueConstraint))
                .GroupBy(c => c.UniqueConstraint))
            {
                commandText += string.Format(CultureInfo.InvariantCulture, ", {0} unique",
                    Environment.NewLine);
                commandText += "(";

                columnDelimiterIsNeeded = false;
                foreach (string column in uniqueColumns.Select(c => c.Name))
                {
                    if (columnDelimiterIsNeeded) 
                        commandText += ",";
                    commandText += string.Format(CultureInfo.InvariantCulture, "{0}", column);
                    columnDelimiterIsNeeded = true;
                }
                commandText += ")";
            }

            commandText += Environment.NewLine;
            commandText += string.Format(CultureInfo.InvariantCulture, "){0}", Environment.NewLine);

            yield return commandText;
        }

        public IEnumerable<string> DropColumn(string tableName, string columnName)
        {
            return string.Format(CultureInfo.InvariantCulture, "ALTER TABLE {0} DROP {1}", Escape(tableName), Escape(columnName)).Yield();
        }

        public IEnumerable<string> DropDefault(string tableName, Column column)
        {
            Debug.Assert(column.DefaultValue == null);
            return DropDefaultConstraint(tableName, column.Name, false);
        }

        public IEnumerable<string> DropForeignKey(string tableName, string constraintName)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> DropIndex(string tableName, string indexName)
        {
            yield return string.Format(CultureInfo.InvariantCulture, "DROP INDEX {0} ON [dbo].{1} WITH ( ONLINE = OFF )", Escape(indexName), Escape(tableName));
        }

        public IEnumerable<string> DropPrimaryKey(string tableName, string constraintName)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> DropTable(string tableName)
        {
            yield return string.Format(CultureInfo.InvariantCulture, "DROP TABLE {0}", Escape(tableName));
        }

        public IEnumerable<string> DropUniqueConstraint(string tableName, string constraintName)
        {
            yield return DropConstraint(tableName, constraintName);
        }

        public string ExistsTable(string databaseName, string tableName)
        {
            return string.Format(CultureInfo.InvariantCulture,
              @"SELECT COUNT(*) FROM systables WHERE tabname={0}",
                ValueEscape(tableName));
        }

        public IEnumerable<string> RenameColumn(string tableName, string oldName, string newName)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> RenamePrimaryKey(string tableName, string oldName, string newName)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> RenameTable(string oldName, string newName)
        {
            throw new NotImplementedException();
        }

        protected string GetTypeSpecifier(DataType type)
        {
            switch (type.DbType)
            {
                //case DbType.AnsiString:
                //    if (type.Size > 0)
                //    {
                //        return string.Format(CultureInfo.InvariantCulture, "[varchar]({0})", type.Size);
                //    }
                //    else
                //    {
                //        return "[varchar](max)";
                //    }
                case DbType.Binary:
                    return "BLOB";
                case DbType.Byte:
                    return "[smallint]";
                case DbType.Boolean:
                    return "BOOLEAN";
                //case DbType.Currency:
                //    break;
                case DbType.Date:
                    return "[date]";
                case DbType.DateTime:
                    return "datetime year to fraction(5)";
                case DbType.Decimal:
                    return string.Format(CultureInfo.InvariantCulture, "DECIMAL({0},{1})", type.Size, type.Scale);
                case DbType.Double:
                    return "[float]";
                case DbType.Guid:
                    return "CHAR(36)";
                case DbType.Int16:
                    return "SMALLINT";
                case DbType.Int32:
                    return "INTEGER";
                case DbType.Int64:
                    return "[bigint]";
                //case DbType.Object:
                //    break;
                case DbType.SByte:
                    return "[tinyint]";
                case DbType.Single:
                    return "[real]";
                case DbType.String:
                    if (type.Size > 0)
                    {
                        if (type.Size < 255)
                            return string.Format(CultureInfo.InvariantCulture, "NVARCHAR({0})", type.Size);
                        if (type.Size < 10000)
                            return string.Format(CultureInfo.InvariantCulture, "LVARCHAR({0})", type.Size);
                    }                    
                    return "TEXT";
                case DbType.Time:
                    return "[time]";
                //case DbType.UInt16:
                //    break;
                //case DbType.UInt32:
                //    break;
                //case DbType.UInt64:
                //    break;
                case DbType.VarNumeric:
                    return string.Format(CultureInfo.InvariantCulture, "[numeric]({0}, {1})", type.Size, type.Scale);
                //case DbType.AnsiStringFixedLength:
                //    return string.Format(CultureInfo.InvariantCulture, "[char]({0})", type.Size);
                case DbType.StringFixedLength:
                    return string.Format(CultureInfo.InvariantCulture, "nchar({0})", type.Size);
                //case DbType.Xml:
                //    break;
                //case DbType.DateTime2:
                //    return "[datetime2]";
                //case DbType.DateTimeOffset:
                //    return "[datetimeoffset]";
                default:
                    throw new ArgumentOutOfRangeException("type");
            }
        }
    }
}
