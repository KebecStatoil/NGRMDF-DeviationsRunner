using System;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Services.AppAuthentication;
using RestSharp;
using RestSharp.Serialization;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Linq;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace NGRMDF_DeviationsRunner
{
    class Program
    {
        static void Main(string[] args)
        {
            string databricksToken = GetSecret("ngrmdfvtkvdev", "DatabricksToken");
            string databricksdeviationsCalculationJobName = "BatchedDeviations_2_0_1";
            int surveyId = 28;

            ILogger log = new ConsoleLogger();

            log.LogInformation($"SurveyID: {surveyId}");

            Databricks dbUtil = new Databricks(databricksToken, "northeurope.azuredatabricks.net", log);
            JobList dbJobs = dbUtil.ListJobs();
            int? jobId = dbUtil.GetJobIdByName(dbJobs, databricksdeviationsCalculationJobName);

            int? runId = null;

            for (int i = 0; i < 199; i++)
            {
                var deviationRuns = dbUtil.GetActiveRuns(jobId);
                var surveyRuns = GetSurveyRuns(surveyId, deviationRuns, log);
                if (surveyRuns.Count == 0)
                {
                    if (runId != null) // previous run exists and is completed
                    {
                        string output = dbUtil.GetRunNotebookOutput(runId);
                        JObject o = JObject.Parse(output);
                        string result = o.SelectToken("notebook_output.result").ToString();
                        log.LogInformation(result);
                        if (result == "No sequences to calculate. Exiting." || result == "Nothing to process. Exiting.")
                        {
                            break;
                        }
                    }

                    log.LogInformation($"{i}: No runs for SurveyID: {surveyId}. Starting.");

                    var notebookParams = new Dictionary<string, dynamic> { { "survey_id", surveyId } };
                    RunNowJob jobRun = new RunNowJob((int)jobId, notebookParams);
                    RunNowResponse response = dbUtil.RunJob(jobRun);


                    runId = response.RunId;
                }
                else
                {
                    log.LogInformation($"{i}: {surveyRuns.Count} run(s) for SurveyID: {surveyId} in progress. Skipping.");
                }

                Thread.Sleep(5000); // the NGRMDFVT_DeviationsCalculationRunner kicks off every 5 seconds
            }

        }

        public static List<RunsListRun> GetSurveyRuns(int surveyId, List<RunsListRun> deviationRuns, ILogger log)
        {
            List<string> activeRunLifeCycleStates = new List<string>() { "PENDING", "RUNNING", "TERMINATING" };

            log.LogInformation($"Getting runs for SurveyID: {surveyId}.");
            var surveyRuns = deviationRuns
                .Where(x => int.Parse(x.OverridingParameters.NotebookParams["survey_id"]) == surveyId
                    && activeRunLifeCycleStates.Contains(x.State?.LifeCycleState?.ToString())).ToList();

            return surveyRuns;
        }

        static string GetSecret(string vaultName, string secretName)
        {
            AzureServiceTokenProvider azureServiceTokenProvider = new AzureServiceTokenProvider();
            KeyVaultClient keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
            var secret = keyVaultClient.GetSecretAsync($"https://{vaultName}.vault.azure.net/secrets/{secretName}")
                    .ConfigureAwait(false).GetAwaiter().GetResult();
            return secret.Value;
        }
    }

    public class Databricks
    {

        private readonly IRestClient restClient = null;
        private readonly string authorizationToken = string.Empty;
        private readonly ILogger log = null;

        public Databricks(string token, string databricksHost, ILogger logger)
        {
            restClient = new RestClient($"https://{databricksHost}/api/").UseSerializer(() => new JsonNetSerializer());
            authorizationToken = token;
            log = logger;
        }

        public List<RunsListRun> GetActiveRuns(int? jobId = null, bool activeOnly = true, int offset = 0, int limit = 1000)
        {

            List<RunsListRun> allRuns = new List<RunsListRun>();
            int loopOffset = offset;

            RestRequest request = new RestRequest($"2.0/jobs/runs/list");
            if (jobId != null)
            {
                request.AddParameter(new Parameter("job_id", jobId, ParameterType.QueryString));
            }
            if (activeOnly == true)
            {
                request.AddParameter(new Parameter("active_only", "true", ParameterType.QueryString));
            }
            request.AddParameter(new Parameter("offset", offset, ParameterType.QueryString));
            request.AddParameter(new Parameter("limit", limit, ParameterType.QueryString));
            request.AddHeader("Authorization", $"Bearer {authorizationToken}");

            IRestResponse<RunsList> response = restClient.Get<RunsList>(request);
            if (response.Data?.Runs != null && response.Data.Runs.Count > 0)
            {
                allRuns.AddRange(response.Data.Runs);
                while (response.Data.HasMore)
                {
                    loopOffset += limit;
                    request.AddOrUpdateParameter(new Parameter("offset", loopOffset, ParameterType.QueryString));
                    response = restClient.Get<RunsList>(request);
                    allRuns.AddRange(response.Data.Runs);
                }
            }
            log.LogInformation($"Collected {allRuns.Count} active Databricks jobs runs.");

            return allRuns;
        }

        public int CountActiveRuns(int? jobId = null, bool activeOnly = true, int offset = 0, int limit = 1000)
        {

            List<RunsListRunSimple> allRuns = new List<RunsListRunSimple>();
            int loopOffset = offset;

            RestRequest request = new RestRequest($"2.0/jobs/runs/list");
            if (jobId != null)
            {
                request.AddParameter(new Parameter("job_id", jobId, ParameterType.QueryString));
            }
            if (activeOnly == true)
            {
                request.AddParameter(new Parameter("active_only", "true", ParameterType.QueryString));
            }
            request.AddParameter(new Parameter("offset", offset, ParameterType.QueryString));
            request.AddParameter(new Parameter("limit", limit, ParameterType.QueryString));
            request.AddHeader("Authorization", $"Bearer {authorizationToken}");

            IRestResponse<RunsListSimple> response = restClient.Get<RunsListSimple>(request);
            if (response.Data?.Runs != null && response.Data.Runs.Count > 0)
            {
                allRuns.AddRange(response.Data.Runs);
                while (response.Data.HasMore)
                {
                    loopOffset += limit;
                    request.AddOrUpdateParameter(new Parameter("offset", loopOffset, ParameterType.QueryString));
                    response = restClient.Get<RunsListSimple>(request);
                    allRuns.AddRange(response.Data.Runs);
                }
            }
            log.LogInformation($"Collected {allRuns.Count} active Databricks jobs runs.");

            return allRuns.Count;
        }

        public RunNowResponse RunJob(RunNowJob job)
        {

            RestRequest request = new RestRequest("2.0/jobs/run-now");
            request.AddHeader("Authorization", $"Bearer {authorizationToken}");
            request.AddJsonBody(job);

            IRestResponse<RunNowResponse> response = restClient.Post<RunNowResponse>(request);
            if (response.StatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new ApplicationException($"({response.StatusCode}) Could not start a new run: {response.Content} ");
            }

            return response.Data;
        }

        public RunsListRun GetJobRunStatus(int runId)
        {
            string[] runStatuses = { "PENDING", "RUNNING", "TERMINATING" };

            RestRequest request = new RestRequest("2.0/jobs/runs/get");
            request.AddHeader("Authorization", $"Bearer {authorizationToken}");
            request.AddParameter("run_id", runId, ParameterType.QueryStringWithoutEncode);

            IRestResponse<RunsListRun> response = restClient.Get<RunsListRun>(request);

            while (runStatuses.Any(x => x == response.Data.State.LifeCycleState))
            {
                Thread.Sleep(2000);
                response = restClient.Get<RunsListRun>(request);
            }

            return response.Data;
        }

        public string GetRunNotebookOutput(int? runId)
        {
            RestRequest request = new RestRequest($"2.0/jobs/runs/get-output");
            request.AddHeader("Authorization", $"Bearer {authorizationToken}");
            request.AddParameter(new Parameter("run_id", runId, ParameterType.QueryString));

            IRestResponse response = restClient.Get(request);

            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(response.Content);
            Console.ResetColor();

            return response.Content;
        }

        public JobList ListJobs()
        {

            RestRequest request = new RestRequest("2.0/jobs/list");
            request.AddHeader("Authorization", $"Bearer {authorizationToken}");

            IRestResponse<JobList> response = restClient.Get<JobList>(request);

            return response.Data;
        }

        public int? GetJobIdByName(JobList jobs, string jobName)
        {

            int? jobId = null;

            Job job = jobs.Jobs.Where(x => x.Settings.Name == jobName).FirstOrDefault();
            jobId = job?.JobId;

            return jobId;
        }
    }

    public class JsonNetSerializer : IRestSerializer
    {
        public string Serialize(object obj) =>
            JsonConvert.SerializeObject(obj);

        public string Serialize(Parameter parameter) =>
            JsonConvert.SerializeObject(parameter.Value);

        public T Deserialize<T>(IRestResponse response) =>
            JsonConvert.DeserializeObject<T>(response.Content);

        public string[] SupportedContentTypes { get; } = { "application/json", "text/json", "text/x-json", "text/javascript", "*+json" };

        public string ContentType { get; set; } = "application/json";

        public DataFormat DataFormat { get; } = DataFormat.Json;
    }

    public class Job
    {
        [JsonProperty("job_id")]
        public int JobId { get; set; }

        [JsonProperty("created_time")]
        public ulong CreatedTime { get; set; }

        [JsonProperty("creator_user_name")]
        public string CreatorUserName { get; set; }

        [JsonProperty("settings")]
        public JobSettings Settings { get; set; }
    }

    public class JobSettings
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("existing_cluster_id")]
        public string Existing_ClusterId { get; set; }

        [JsonProperty("email_notifications")]
        public JobEmailNotifications EmailNotifications { get; set; }

        [JsonProperty("timeout_seconds")]
        public int TimeoutSeconds { get; set; }

        [JsonProperty("max_retries")]
        public int MaxRetries { get; set; }

        [JsonProperty("max_concurrent_runs")]
        public int MaxConcurrentRuns { get; set; }

        [JsonProperty("notebook_task")]
        public NotebookTask NotebookTask { get; set; }
    }

    public class JobEmailNotifications
    {
        [JsonProperty("on_start")]
        public string[] OnStart { get; set; }

        [JsonProperty("on_success")]
        public string[] OnSuccess { get; set; }

        [JsonProperty("on_failure")]
        public string[] OnFailure { get; set; }

        [JsonProperty("no_alert_for_skipped_runs")]
        public bool NoAlertForSkippedRuns { get; set; }
    }

    public class JobList
    {
        [JsonProperty("jobs")]
        public List<Job> Jobs { get; set; }
    }

    public class RunNowJob
    {

        [JsonProperty("job_id")]
        public int JobId { get; set; }

        [JsonProperty("notebook_params")]
        public Dictionary<string, dynamic> NotebookParams { get; set; }

        public RunNowJob(int jobId, Dictionary<string, dynamic> notebookParams)
        {
            JobId = jobId;
            NotebookParams = notebookParams;
        }
    }

    public class RunNowResponse
    {
        [JsonProperty("run_id")]
        public int RunId { get; set; }

        [JsonProperty("number_in_job")]
        public int NumberInJob { get; set; }
    }

    public class RunsList
    {

        [JsonProperty("runs")]
        public List<RunsListRun> Runs { get; set; }

        [JsonProperty("has_more")]
        public bool HasMore { get; set; }
    }

    public class RunsListSimple
    {

        [JsonProperty("runs")]
        public List<RunsListRunSimple> Runs { get; set; }

        [JsonProperty("has_more")]
        public bool HasMore { get; set; }
    }
    public class RunsListRunSimple
    {

        [JsonProperty("job_id")]
        public int JobId { get; set; }

        [JsonProperty("run_id")]
        public int RunId { get; set; }

        [JsonProperty("state")]
        public RunState State { get; set; }

        [JsonProperty("task")]
        public Task Task { get; set; }

    }

    public class RunState
    {

        [JsonProperty("life_cycle_state")]
        public string LifeCycleState { get; set; }

        [JsonProperty("result_state")]
        public string ResultState { get; set; }

        [JsonProperty("state_message")]
        public string StateMessage { get; set; }
    }

    public class RunsListRun
    {
        [JsonProperty("job_id")]
        public int JobId { get; set; }

        [JsonProperty("run_id")]
        public int RunId { get; set; }

        [JsonProperty("number_in_job")]
        public int NumberInJob { get; set; }

        [JsonProperty("original_attempt_run_id")]
        public int OriginalAttemptRunId { get; set; }

        [JsonProperty("state")]
        public State State { get; set; }

        [JsonProperty("task")]
        public Task Task { get; set; }

        [JsonProperty("cluster_spec")]
        public ClusterSpec ClusterSpec { get; set; }

        [JsonProperty("cluster_instance")]
        public ClusterInstance ClusterInstance { get; set; }

        [JsonProperty("overriding_parameters")]
        public OverridingParameters OverridingParameters { get; set; }

        [JsonProperty("start_time")]
        public ulong StartTime { get; set; }

        [JsonProperty("setup_duration")]
        public int SetupDuration { get; set; }

        [JsonProperty("execution_duration")]
        public int ExecutionDuration { get; set; }

        [JsonProperty("cleanup_duration")]
        public int CleanupDuration { get; set; }

        [JsonProperty("trigger")]
        public string Trigger { get; set; }

        [JsonProperty("creator_user_name")]
        public string CreatorUserName { get; set; }

        [JsonProperty("run_name")]
        public string RunName { get; set; }

        [JsonProperty("run_page_url")]
        public string RunPageUrl { get; set; }

        [JsonProperty("run_type")]
        public string RunType { get; set; }
    }

    public class State
    {
        [JsonProperty("life_cycle_state")]
        public string LifeCycleState { get; set; }

        [JsonProperty("result_state")]
        public string ResultState { get; set; }

        [JsonProperty("state_message")]
        public string StateMessage { get; set; }
    }

    public class Task
    {
        [JsonProperty("notebook_task")]
        public NotebookTask NotebookTask { get; set; }
    }

    public class ClusterSpec
    {
        [JsonProperty("existing_cluster_id")]
        public string ExistingClusterId { get; set; }
    }

    public class ClusterInstance
    {
        [JsonProperty("cluster_id")]
        public string ClusterId { get; set; }

        [JsonProperty("spark_context_id")]
        public string SparkContextId { get; set; }
    }

    public class OverridingParameters
    {
        [JsonProperty("notebook_params")]
        public Dictionary<string, dynamic> NotebookParams { get; set; }
    }

    public class NotebookTask
    {
        [JsonProperty("notebook_path")]
        public string NotebookPath { get; set; }

        [JsonProperty("base_parameters")]
        public Dictionary<string, dynamic> BaseParameters { get; set; }
    }

    public class ConsoleLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state)
        {
            throw new NotImplementedException();
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            throw new NotImplementedException();
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            Console.WriteLine(state.ToString());
        }
    }

}