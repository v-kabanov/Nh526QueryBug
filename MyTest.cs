using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Linq;
using NHibernate;
using NUnit.Framework;
using NHibernate.Cfg;
using NHibernate.Dialect;
using NHibernate.Driver;
using NHibernate.Hql.Ast.ANTLR;
using NHibernate.Linq;
using NHibernate.Tool.hbm2ddl;

namespace Nh526QueryBug
{
    public static class ConnectionExtensions
    {
        public static int ExecuteNonQuery(this SqlConnection cn, string query)
        {
            if (cn.State != ConnectionState.Open)
                cn.Open();

            using (var cm = cn.CreateCommand())
            {
                cm.CommandText = query;
                cm.CommandType = CommandType.Text;

                return cm.ExecuteNonQuery();
            }
        }
    }

    [TestFixture]
    public class MyTest
    {
        const string ConnectionStringInMemory = "Data Source=:memory:;Version=3;New=True;";
        const string ConnectionStringMsSqlSetup = "Server=.; Database=master; Integrated Security=yes;";
        const string MsSqlDatabaseName = "NhTest";
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

    <class name=""Material"">
        <id name=""Id"" type=""Int32"">
            <generator class=""assigned""/>
        </id>
        <version name=""IntegrityVersion"" column=""IntegrityVersion"" unsaved-value=""0"" />

        <many-to-one name=""MaterialDefinition"" class=""MaterialDefinition"" column=""MaterialDefinitionId"" not-null=""true"" not-found=""exception"" />
        <many-to-one name=""ProductDefinition"" class=""ProductDefinition"" column=""ProductDefinitionId"" not-null=""true"" not-found=""exception"" />
        
        <property name=""Name"" type=""string"" />
    </class>

    <class name=""MaterialDefinition"">
        <id name=""Id"" type=""Int32"">
            <generator class=""assigned""/>
        </id>
        <version name=""IntegrityVersion"" column=""IntegrityVersion"" unsaved-value=""0"" />

        <property name=""Name"" type=""string"" />
    </class>

    <class name=""ProductDefinition"">
        <id name=""Id"" type=""Int32"">
            <generator class=""assigned""/>
        </id>
        <version name=""IntegrityVersion"" column=""IntegrityVersion"" unsaved-value=""0"" />
        <many-to-one name=""MaterialDefinition"" class=""MaterialDefinition"" column=""MaterialDefinitionId"" not-null=""true"" not-found=""exception"" />

        <property name=""Name"" type=""string"" />
    </class>

</hibernate-mapping>";

        private ISessionFactory SessionFactorySqLite { get; set; }

        private ISessionFactory _sessionFactoryMs;

        private ISessionFactory SessionFactoryMs
        {
            get
            {
                if (_sessionFactoryMs == null)
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

                    OneTimeEnsureTestSqlDatabaseExists(configuration);

                    _sessionFactoryMs = configuration.BuildSessionFactory();
                }

                return _sessionFactoryMs;
            }
        }

        private ISession OpenSqLiteSession() => OpenSession(new SQLiteConnection(ConnectionStringInMemory), SessionFactorySqLite);

        private ISession OpenMsSqlSession() => OpenSession(new SqlConnection(ConnectionStringMsSql), SessionFactoryMs);

        [OneTimeSetUp]
        public void SetUp()
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

            SessionFactorySqLite = configuration.BuildSessionFactory();

            using (var connection = new SQLiteConnection(ConnectionStringInMemory))
            {
                connection.Open();

                new SchemaExport(configuration).Execute(false, true, false, connection, TestContext.Progress);
            }
        }

        [Test]
        public void TestSelectManySqLite()
        {
            using (var session = OpenSqLiteSession())
            {
                TestQueryInternal(session);
            }
        }

        [Test]
        public void TestSelect()
        {
            using (var session = OpenSqLiteSession())
            {
                var selectedProductDefinition = new ProductDefinition() { Id = 1000, MaterialDefinition = new MaterialDefinition { Id = 1 } };
                session.Save(selectedProductDefinition.MaterialDefinition);
                session.Save(selectedProductDefinition);

                var selectedProducts = new[] { selectedProductDefinition };

                var query = session.Query<Material>()
                    .Where(x => selectedProducts.Contains(x.ProductDefinition) && selectedProducts.Select(y => y.MaterialDefinition).Contains(x.MaterialDefinition));

                var sessionImpl = session.GetSessionImplementation();
                var factoryImplementor = sessionImpl.Factory;

                var nhLinqExpression = new NhLinqExpression(query.Expression, factoryImplementor);
                var translatorFactory = new ASTQueryTranslatorFactory();
                var translator = translatorFactory.CreateQueryTranslators(nhLinqExpression, null, false, sessionImpl.EnabledFilters, factoryImplementor).First();

                TestContext.WriteLine(translator.SQLString);

                Assert.IsNotEmpty(nhLinqExpression.ParameterValuesByName.Values.Where(x => (x.Item1 as IList<ProductDefinition>)?.Contains(selectedProductDefinition) == true));
                Assert.IsNotEmpty(nhLinqExpression.ParameterValuesByName.Values.Where(x => (x.Item1 as IList<MaterialDefinition>)?.Contains(selectedProductDefinition.MaterialDefinition) == true));

                //var result = query.ToList();
            }
        }

        [Test]
        public void TestMsSql()
        {
            using (var session = OpenMsSqlSession())
            {
                TestQueryInternal(session);
            }
        }

        private ISession OpenSession(DbConnection connection, ISessionFactory factory)
        {
            if (connection.State != ConnectionState.Open)
                connection.Open();
            var session = factory.WithOptions().Connection(connection).OpenSession();
            return session;
        }

        private void OneTimeEnsureTestSqlDatabaseExists(Configuration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));

            using (var cn = new SqlConnection(ConnectionStringMsSqlSetup))
            {
                cn.Open();
                cn.ExecuteNonQuery($@"if db_id(N'{MsSqlDatabaseName}') is not null drop database [{MsSqlDatabaseName}]; create database [{MsSqlDatabaseName}];");

                cn.ChangeDatabase(MsSqlDatabaseName);

                new SchemaExport(configuration).Execute(false, true, false, cn, TestContext.Progress);
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
    public class EntityWithName : Entity
    {
        public virtual string Name { get; set; }
    }

    public class Material : EntityWithName
    {

        public virtual MaterialDefinition MaterialDefinition { get; set; }

        public virtual ProductDefinition ProductDefinition { get; set; }
    }

    public class MaterialDefinition : EntityWithName
    {
    }

    public class ProductDefinition : EntityWithName
    {
        public virtual MaterialDefinition MaterialDefinition { get; set; }
    }
}
