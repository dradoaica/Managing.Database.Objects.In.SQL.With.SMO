using Microsoft.Extensions.Configuration;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using Index = Microsoft.SqlServer.Management.Smo.Index;

namespace SimpleSMO
{
    internal class Program
    {
        private static readonly string DROP_CONSTRAINT = "ALTER TABLE {0} DROP CONSTRAINT {1}";
        private static readonly string ADD_PK_CONSTRAINT = "ALTER TABLE {0} ADD CONSTRAINT {1} PRIMARY KEY ({2})";
        private static readonly string ADD_FK_CONSTRAINT = "ALTER TABLE {0} ADD CONSTRAINT {1} FOREIGN KEY ({2}) REFERENCES {3} ({4})";
        private static readonly FileStream _dropfksfs = new FileStream(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "dropfks.sql"), FileMode.Create, FileAccess.Write);
        private static readonly StreamWriter _dropfksw = new StreamWriter(_dropfksfs);
        private static readonly FileStream _droppksfs = new FileStream(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "droppks.sql"), FileMode.Create, FileAccess.Write);
        private static readonly StreamWriter _droppksw = new StreamWriter(_droppksfs);
        private static readonly FileStream _addpksfs = new FileStream(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "addpks.sql"), FileMode.Create, FileAccess.Write);
        private static readonly StreamWriter _addpksw = new StreamWriter(_addpksfs);
        private static readonly FileStream _addfksfs = new FileStream(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "addfks.sql"), FileMode.Create, FileAccess.Write);
        private static readonly StreamWriter _addfksw = new StreamWriter(_addfksfs);
        private static readonly FileStream _spsfs = new FileStream(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "sps.sql"), FileMode.Create, FileAccess.Write);
        private static readonly StreamWriter _spsw = new StreamWriter(_spsfs);

        private static void ProcessLevel(Level lvl, Database db, Server srv)
        {
            foreach (Level level in lvl.Levels)
            {
                Table table = db.Tables[level.Name];
                if (table != null)
                {
                    Index oldpk = table.Indexes.Cast<Index>().FirstOrDefault(index => index.IndexKeyType == IndexKeyType.DriPrimaryKey);
                    if (oldpk != null && oldpk.IndexedColumns.Count < 2)
                    {
                        DependencyWalker dw = new DependencyWalker(srv);
                        DependencyTree dependencies = dw.DiscoverDependencies(new SqlSmoObject[] { table }, DependencyType.Children);
                        DependencyTreeNode current = dependencies.FirstChild.FirstChild;

                        List<ForeignKey> oldfks = new List<ForeignKey>();
                        List<ForeignKey> newfks = new List<ForeignKey>();

                        _spsw.WriteLine();
                        _spsw.Write(table.Name + ":");

                        bool checkself = false;
                        while (current != null)
                        {
                            if (srv.GetSmoObject(current.Urn) is StoredProcedure sp)
                            {
                                _spsw.Write(sp.Name + "; ");
                            }

                            if (srv.GetSmoObject(current.Urn) is Table dependency)
                            {
                                foreach (ForeignKey oldfk in dependency.ForeignKeys)
                                {
                                    if (oldfk.ReferencedTable == table.Name)
                                    {
                                        _dropfksw.WriteLine(string.Format(DROP_CONSTRAINT, dependency.Name, oldfk.Name));

                                        ForeignKey newfk = new ForeignKey(dependency, oldfk.Name);

                                        string columns1 = string.Empty;
                                        string columns2 = string.Empty;
                                        bool _first = true;

                                        foreach (ForeignKeyColumn fkc in oldfk.Columns)
                                        {
                                            columns1 += (_first ? "" : ", ") + fkc.Name;
                                            columns2 += (_first ? "" : ", ") + fkc.ReferencedColumn;
                                            _first = false;
                                            newfk.Columns.Add(new ForeignKeyColumn(newfk, fkc.Name, fkc.ReferencedColumn));
                                        }

                                        if (level.Parent != null)
                                        {
                                            foreach (ForeignKey fk in dependency.ForeignKeys)
                                            {
                                                if (fk.ReferencedTable == level.Parent.Name)
                                                {
                                                    foreach (ForeignKeyColumn fkc in fk.Columns)
                                                    {
                                                        columns1 += (_first ? "" : ", ") + fkc.Name;
                                                        columns2 += (_first ? "" : ", ") + fkc.ReferencedColumn;
                                                        _first = false;
                                                        newfk.Columns.Add(new ForeignKeyColumn(newfk, fkc.Name, fkc.ReferencedColumn));
                                                    }
                                                }
                                            }

                                            if (level.Parent.Parent != null)
                                            {
                                                foreach (ForeignKey fk in dependency.ForeignKeys)
                                                {
                                                    if (fk.ReferencedTable == level.Parent.Parent.Name)
                                                    {
                                                        foreach (ForeignKeyColumn fkc in fk.Columns)
                                                        {
                                                            columns1 += (_first ? "" : ", ") + fkc.Name;
                                                            columns2 += (_first ? "" : ", ") + fkc.ReferencedColumn;
                                                            _first = false;
                                                            newfk.Columns.Add(new ForeignKeyColumn(newfk, fkc.Name, fkc.ReferencedColumn));
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        newfk.ReferencedTable = oldfk.ReferencedTable;
                                        newfk.ReferencedTableSchema = oldfk.ReferencedTableSchema;

                                        _addfksw.WriteLine(string.Format(ADD_FK_CONSTRAINT, dependency.Name, oldfk.Name, columns1, oldfk.ReferencedTable, columns2));

                                        oldfks.Add(oldfk);
                                        newfks.Add(newfk);
                                    }
                                }
                            }

                            current = current.NextSibling;

                            if (current == null && !checkself)
                            {
                                current = dependencies.FirstChild;
                                checkself = true;
                            }
                        }

                        _droppksw.WriteLine(string.Format(DROP_CONSTRAINT, table.Name, oldpk.Name));

                        Index newpk = new Index(table, oldpk.Name);

                        string columns = string.Empty;
                        bool first = true;

                        foreach (IndexedColumn ic in oldpk.IndexedColumns)
                        {
                            columns += (first ? "" : ", ") + ic.Name;
                            first = false;
                            newpk.IndexedColumns.Add(new IndexedColumn(newpk, ic.Name));
                        }

                        if (level.Parent != null)
                        {
                            foreach (ForeignKey fk in table.ForeignKeys)
                            {
                                if (fk.ReferencedTable == level.Parent.Name)
                                {
                                    foreach (ForeignKeyColumn fkc in fk.Columns)
                                    {
                                        columns += (first ? "" : ", ") + fkc.Name;
                                        first = false;
                                        newpk.IndexedColumns.Add(new IndexedColumn(newpk, fkc.Name));
                                    }
                                }
                            }

                            if (level.Parent.Parent != null)
                            {
                                foreach (ForeignKey fk in table.ForeignKeys)
                                {
                                    if (fk.ReferencedTable == level.Parent.Parent.Name)
                                    {
                                        foreach (ForeignKeyColumn fkc in fk.Columns)
                                        {
                                            columns += (first ? "" : ", ") + fkc.Name;
                                            first = false;
                                            newpk.IndexedColumns.Add(new IndexedColumn(newpk, fkc.Name));
                                        }
                                    }
                                }
                            }
                        }

                        newpk.IsClustered = true;
                        newpk.IsUnique = true;
                        newpk.IndexKeyType = IndexKeyType.DriPrimaryKey;

                        _addpksw.WriteLine(string.Format(ADD_PK_CONSTRAINT, table.Name, oldpk.Name, columns));

                        ProcessLevel(level, db, srv);

                        Console.Write(table.Name + "\r\n");
                        try
                        {
                            foreach (ForeignKey oldfk in oldfks)
                            {
                                oldfk.Drop();
                                table.Alter();
                            }

                            oldpk.Drop();
                            table.Alter();

                            newpk.Create();

                            foreach (ForeignKey newfk in newfks)
                            {
                                newfk.Create();
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.Write(ex.Message + "\r\n");
                        }
                    }
                }
            }
        }

        private static void Main(string[] args)
        {
            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(config.GetConnectionString("db"));
            SqlConnection sqlconn = new SqlConnection(builder.ConnectionString);
            ServerConnection conn = new ServerConnection(sqlconn);
            Server srv = new Server(conn);
            Database db = srv.Databases[builder.InitialCatalog];

            Level lvl0;
            List<Level> list = new List<Level>();

            lvl0 = new Level("Table1");
            list.Add(lvl0);
            lvl0.Add(new Level("Table11"));
            lvl0.Add(new Level("Table12"));

            foreach (Level lvl in list)
            {
                ProcessLevel(lvl, db, srv);
            }

            _droppksw.Flush(); _droppksfs.Flush(); _droppksw.Close(); _droppksfs.Close();
            _dropfksw.Flush(); _dropfksfs.Flush(); _dropfksw.Close(); _dropfksfs.Close();
            _addpksw.Flush(); _addpksfs.Flush(); _addpksw.Close(); _addpksfs.Close();
            _addfksw.Flush(); _addfksfs.Flush(); _addfksw.Close(); _addfksfs.Close();
            _spsw.Flush(); _spsfs.Flush(); _spsw.Close(); _spsfs.Close();

            Console.WriteLine("Press enter to close...");
            Console.ReadLine();
        }
    }
}
