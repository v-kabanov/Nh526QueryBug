using System.IO;
using System.Reflection;
using log4net;
using log4net.Config;
using NUnit.Framework;

namespace Nh526QueryBug
{
    [SetUpFixture]
    public class TestSession
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        [OneTimeSetUp]
        public static void StartSession()
        {
            var log4NetConfigPath = Path.Combine(TestContext.CurrentContext.TestDirectory, "log4net.config");
            XmlConfigurator.ConfigureAndWatch(new FileInfo(log4NetConfigPath));

            Log.Info("Starting test session");
        }
    }
}