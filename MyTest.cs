using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Linq;
using NHibernate;
using NUnit.Framework;
using NHibernate.Cfg;
using NHibernate.Dialect;
using NHibernate.Driver;
using NHibernate.Linq;
using NHibernate.Tool.hbm2ddl;

namespace Nh526QueryBug
{
    [TestFixture]
    public class MyTest
    {

        const string ConnectionStringInMemory = "Data Source=:memory:;Version=3;New=True;";
        const string ConnectionStringMsSql = "Server=.; Database=NhTest; Integrated Security=yes;";

        const string Mapping = @"<hibernate-mapping assembly=""Nh526QueryBug"" namespace=""Nh526QueryBug"" xmlns=""urn:nhibernate-mapping-2.2"">
    <class name=""ChildEntity"">
        <id name=""Id"" type=""Int32"">
            <generator class=""assigned""/>
        </id>
        <version name=""IntegrityVersion"" column=""IntegrityVersion"" unsaved-value=""0"" />
        <property name=""Name"" type=""string"" />
        <many-to-one name=""Parent"" class=""ParentEntity"" column=""ParentId"" not-null=""true"" not-found=""exception"" />

        <set name=""GrandChildren"" lazy=""true"" generic=""true"" inverse=""true"" cascade=""all-delete-orphan"">
            <key column=""ParentId"" />
            <one-to-many class=""GrandChildEntity"" />
        </set>
    </class>

    <class name=""ParentEntity"" table=""ParentEntity"">
        <id name=""Id"" type=""Int32"">
            <generator class=""assigned""/>
        </id>
        <version name=""IntegrityVersion"" column=""IntegrityVersion"" unsaved-value=""0"" />
        <set name=""Children"" lazy=""true"" generic=""true"" inverse=""true"" cascade=""all-delete-orphan"">
            <key column=""ParentId"" />
            <one-to-many class=""ChildEntity"" />
        </set>
    </class>

    <class name=""GrandChildEntity"">
        <id name=""Id"" type=""Int32"">
            <generator class=""assigned""/>
        </id>
        <version name=""IntegrityVersion"" column=""IntegrityVersion"" unsaved-value=""0"" />
        <many-to-one name=""Parent"" class=""ChildEntity"" column=""ParentId"" />
    </class>

</hibernate-mapping>";

        [Test]
        public void TestSqLite()
        {
            var configuration = new Configuration();
            configuration
                .DataBaseIntegration(
                    x =>
                    {
                        x.ConnectionString = ConnectionStringInMemory;
                        x.Driver<SQLite20Driver>();
                        x.Dialect<SQLiteDialect>();
                        x.LogSqlInConsole = true;
                        x.LogFormattedSql = true;
                    })
                .AddXmlString(Mapping);
            configuration.BuildMappings();

            using (var connection = new SQLiteConnection(ConnectionStringInMemory))
            {
                connection.Open();

                SetUpAndRunTestQuery(configuration, connection);
            }
        }

        [Test]
        public void TestMsSql()
        {
            var configuration = new Configuration();
            configuration
                .DataBaseIntegration(
                    x =>
                    {
                        x.ConnectionString = ConnectionStringMsSql;
                        x.Driver<SqlClientDriver>();
                        x.Dialect<MsSql2012Dialect>();
                        x.LogSqlInConsole = true;
                        x.LogFormattedSql = true;
                    })
                .AddXmlString(Mapping);
            configuration.BuildMappings();

            EnsureTestSqlDatabaseExists();

            using (var connection = new SqlConnection(ConnectionStringMsSql))
            {
                connection.Open();

                SetUpAndRunTestQuery(configuration, connection);
            }
        }

        private void SetUpAndRunTestQuery(Configuration configuration, DbConnection connection)
        {
            var sessionFactory = configuration.BuildSessionFactory();

            new SchemaExport(configuration).Execute(false, true, false, connection, TestContext.Progress);
            using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
            {
                var parent = CreateTestParentEntity();
                session.Save(parent);
                session.Flush();
            }

            using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
            {
                TestQueryInternal(session);
            }
        }

        private void EnsureTestSqlDatabaseExists()
        {
            var csBuilder = new SqlConnectionStringBuilder(ConnectionStringMsSql);
            var targetDatabaseName = csBuilder.InitialCatalog;
            csBuilder.InitialCatalog = "master";
            using (var connection = new SqlConnection(csBuilder.ConnectionString))
            {
                connection.Open();

                var sqlCommand = connection.CreateCommand();
                sqlCommand.CommandText = $"if (db_id('{targetDatabaseName}')) is null create database [{targetDatabaseName}]";
                sqlCommand.ExecuteNonQuery();
            }
        }

        private static void TestQueryInternal(ISession session)
        {
            var parentQuery = session.Query<ParentEntity>()
                .Where(x => x.Id == 1);

            parentQuery.FetchMany(x => x.Children)
                .ToFuture();

            // this fails
            Assert.DoesNotThrow(() => parentQuery.SelectMany(x => x.Children)
                .FetchMany(x => x.GrandChildren)
                .ToFuture());

            var parent = parentQuery.ToFuture().SingleOrDefault();

            Assert.IsNotNull(parent);

            Assert.IsTrue(NHibernateUtil.IsInitialized(parent.Children));

            foreach (var child in parent.Children)
            {
                Assert.IsTrue(NHibernateUtil.IsInitialized(child.GrandChildren));
            }
        }

        private static ParentEntity CreateTestParentEntity()
        {
            var parent = new ParentEntity() {Id = 1};
            parent.Children.Add(new ChildEntity() {Id = 1, Parent = parent});
            parent.Children.Add(new ChildEntity() {Id = 2, Parent = parent});
            var childEntity = parent.Children.First();
            childEntity.GrandChildren.Add(new GrandChildEntity() {Id = 1, Parent = childEntity});
            return parent;
        }
    }

    public class Entity
    {
        public virtual int Id { get; set; }
        public virtual int IntegrityVersion { get; set; }
    }

    public class ChildEntity : Entity
    {

        public virtual string Name { get; set; }

        public virtual ParentEntity Parent { get; set; }

        public virtual ISet<GrandChildEntity> GrandChildren { get; set; } = new HashSet<GrandChildEntity>();

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return (obj as ChildEntity)?.Id == Id;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return Id.GetHashCode();
        }
    }

    public class ParentEntity : Entity
    {
        public virtual ISet<ChildEntity> Children { get; set; } = new HashSet<ChildEntity>();
    }

    public class GrandChildEntity : Entity
    {
        public virtual ChildEntity Parent { get; set; }
    }
}
