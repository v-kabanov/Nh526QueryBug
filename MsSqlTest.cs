using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using NHibernate;
using NHibernate.Cfg;
using NHibernate.Dialect;
using NHibernate.Driver;
using NHibernate.Engine;
using NHibernate.Hql.Ast.ANTLR;
using NHibernate.Linq;
using NHibernate.Tool.hbm2ddl;
using NHibernate.Type;
using NUnit.Framework;

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
    public class MsSqlTest
    {
        const string ConnectionStringMsSqlSetup = "Server=.; Database=master; Integrated Security=yes;";
        const string MsSqlDatabaseName = "NhTest";
        const string ConnectionStringMsSql = "Server=.; Database=NhTest; Integrated Security=yes;";

        private ISessionFactory SessionFactory { get; set; }

        public const string Mapping = @"<hibernate-mapping assembly=""Nh526QueryBug"" namespace=""Nh526QueryBug"" xmlns=""urn:nhibernate-mapping-2.2"">
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

        private ISession OpenSession()
        {
            var cn = new SqlConnection(ConnectionStringMsSql);
            var result = SessionFactory.WithOptions().Connection(cn).OpenSession();
            cn.Open();
            return result;
        }

        [OneTimeSetUp]
        public void SetUp()
        {
            using (var cn = new SqlConnection(ConnectionStringMsSqlSetup))
            {
                cn.ExecuteNonQuery($@" if db_id(N'{MsSqlDatabaseName}') is not null drop database [{MsSqlDatabaseName}]; create database [{MsSqlDatabaseName}];");

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

                cn.ChangeDatabase(MsSqlDatabaseName);

                new SchemaExport(configuration).Execute(false, true, false, cn, TestContext.Progress);

                SessionFactory = configuration.BuildSessionFactory();
            }
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            using (var cn = new SqlConnection(ConnectionStringMsSqlSetup))
            {
                cn.ExecuteNonQuery($@"if db_id(N'{MsSqlDatabaseName}') is not null
                    begin
                        alter database [{MsSqlDatabaseName}] set single_user with rollback immediate;
                        drop database [{MsSqlDatabaseName}];
                    end");
            }
        }

        [Test]
        public void TestQuery()
        {
            using (var session = OpenSession())
            {
                var selectedProductDefinition = new ProductDefinition() {Id = 1000, MaterialDefinition = new MaterialDefinition {Id = 1}};
                session.Save(selectedProductDefinition.MaterialDefinition);
                session.Save(selectedProductDefinition);

                var selectedProducts = new [] {selectedProductDefinition};

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
    }

    public class EntityWithName : Entity
    {
        public virtual string Name { get; set; }
    }

    public class Material: EntityWithName
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