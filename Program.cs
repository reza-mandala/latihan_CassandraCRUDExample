using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra;
using Cassandra.Mapping;
using Microsoft.Extensions.Configuration;
using System.Text.Json.Serialization;
using Cassandra.Mapping.Attributes;

namespace IgniteWithCassandraExample
{
    public class Todo
    {
        [JsonPropertyName("id")]
        [Column("id")]
        public Guid Id { get; set; }  // Ganti tipe data menjadi Guid untuk UUID

        [JsonPropertyName("task")]
        [Column("task")]
        public string? Task { get; set; }

        [JsonPropertyName("completed")]
        [Column("completed")]
        public bool Completed { get; set; }

        [JsonPropertyName("user_id")]
        [Column("user_id")]
        public long UserId { get; set; }

        [JsonPropertyName("created_at")]
        [Column("created_at")]
        public DateTime CreatedAt { get; set; }

        [JsonPropertyName("updated_at")]
        [Column("updated_at")]
        public DateTime UpdatedAt { get; set; }
    }

    public class TodoStore // Anda bisa menambahkan interface jika diperlukan
    {
        private readonly ISession _session;

        public TodoStore(ISession? session)
        {
            _session = session ?? throw new ArgumentNullException(nameof(session));
        }

        public Todo? Load(Guid key)
        {
            var statement = new SimpleStatement("SELECT * FROM todos WHERE id = ?", key);
            Row? row = _session?.Execute(statement).FirstOrDefault();

            if (row == null) return null;

            return new Todo
            {
                Id = row.GetValue<Guid>("id"),
                Task = row.GetValue<string>("task"),
                Completed = row.GetValue<bool>("completed"),
                UserId = row.GetValue<long>("user_id"),
                CreatedAt = row.GetValue<DateTime>("created_at"),
                UpdatedAt = row.GetValue<DateTime>("updated_at"),
            };
        }

        public IEnumerable<Todo>? LoadAll(IEnumerable<Guid>? keys = null)
        {
            // Ambil semua todos berdasarkan keys yang diberikan
            var todosQuery = (keys == null || keys.Count() == 0) 
                ? $"SELECT * FROM todos"
                : $"SELECT * FROM todos WHERE id IN ({string.Join(", ", keys)})";
            var todos = _session.Execute(todosQuery).Select(row => new Todo
            {
                Id = row.GetValue<Guid>("id"),
                Task = row.GetValue<string>("task"),
                Completed = row.GetValue<bool>("completed"),
                UserId = row.GetValue<long>("user_id"),
                CreatedAt = row.GetValue<DateTime>("created_at"),
                UpdatedAt = row.GetValue<DateTime>("updated_at"),
            });

            return todos;
        }

        // Implementasi method lain sesuai kebutuhan Anda ...
    }

    class Program
    {
        private static string ContactPoint = "";
        private static string KeyspaceName = "";
        private static Cluster? _cluster;
        private static ISession? _session;

        static void Main()
        {
            // instalasi cassandra:
            // $ docker pull cassandra
            // 
            // menjalankan cassandra (agar bisa mengakses Cassandra dari aplikasi yang berjalan di luar container):
            // $ docker run --name cassandra-container -d -p 9042:9042 cassandra:latest
            //
            // menampilkan container yang sedang berjalan:
            // $ docker ps -a
            //
            // cara mengakses cassandra via terminal:
            // $ docker exec -it cassandra-container cqlsh
            //
            // menghentikan 
            // $ docker stop cassandra-container
            //
            // menghapus container (setelah menghentikannya):
            // $ docker rm cassandra-container

            OpenConnection();
            if (_session == null)
            {
                throw new InvalidOperationException("Session is not initialized");
            }
            DropKeyspace(_session, KeyspaceName);
            InitializeCassandra();
            // create todo
            var uuids = InsertDummyTodos();

            // read todo
            // Uji coba: ambil semua todos dengan ID uuid1 dan uuid2
            var todoStore = new TodoStore(_session);
            var todos = todoStore.LoadAll(uuids);
            if (todos != null)
            {
                foreach (var todo in todos)
                {
                    Console.WriteLine($"{todo.Id}. {todo.Task} (Completed: {todo.Completed}, User: {todo.UserId})");
                }
            }
            
            // update todo
            // Pilih UUID secara acak dari list
            Random random = new Random();
            Guid selectedUuid = uuids[random.Next(uuids.Count)];
            uuids.Remove(selectedUuid);
            UpdateTodo(selectedUuid);            
            var updatedTodo = todoStore.Load(selectedUuid);
            if (updatedTodo != null)
            {
                Console.WriteLine($"{updatedTodo.Id}. {updatedTodo.Task} (Completed: {updatedTodo.Completed}, User: {updatedTodo.UserId})");
            }

            // delete todo
            Guid selectedUuidToDelete = uuids[random.Next(uuids.Count)];
            DeleteTodo(selectedUuidToDelete);
            var allTodos = todoStore.LoadAll();
            if (allTodos != null)
            {
                foreach (var todo in allTodos)
                {
                    Console.WriteLine($"{todo.Id}. {todo.Task} (Completed: {todo.Completed}, User: {todo.UserId})");
                }
            }
        }

        static void DropKeyspace(ISession session, string? keyspaceName)
        {
            try
            {
                session.Execute($"DROP KEYSPACE {keyspaceName};");
                Console.WriteLine($"Keyspace {keyspaceName} dropped successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }
        
        private static void OpenConnection()
        {
            // Membaca konfigurasi dari appsettings.json.
            var configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            ContactPoint = configuration["Cassandra:ContactPoint"] ?? "";
            KeyspaceName = configuration["Cassandra:KeyspaceName"] ?? "";
            Console.WriteLine($"ContactPoint: {ContactPoint}");
            Console.WriteLine($"KeyspaceName: {KeyspaceName}");

            _cluster = Cluster.Builder().AddContactPoint(ContactPoint).Build();
            _session = _cluster.Connect();
        }

        private static void InitializeCassandra()
        {
            var createKeyspaceQuery = $"CREATE KEYSPACE IF NOT EXISTS {KeyspaceName} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};";
            _session?.Execute(createKeyspaceQuery);
            _session?.ChangeKeyspace(KeyspaceName);

            var createTableQuery = @"
                CREATE TABLE IF NOT EXISTS todos (
                    id UUID PRIMARY KEY,
                    task TEXT,
                    completed BOOLEAN,
                    user_id BIGINT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                );
            ";
            _session?.Execute(createTableQuery);

            _session?.Execute("CREATE INDEX IF NOT EXISTS ON todos (completed);");
            _session?.Execute("CREATE INDEX IF NOT EXISTS ON todos (user_id);");
        }

        private static List<Guid> InsertDummyTodos()
        {
            var uuids = new List<Guid>();
            var todos = new List<Todo>
            {
                new Todo {Task = "Do something nice for someone I care about", Completed = true, UserId = 26},
                new Todo {Task = "Memorize the fifty states and their capitals", Completed = false, UserId = 48},
                new Todo {Task = "Watch a classic movie", Completed = false, UserId = 4}
            };

            // Prepare the statements
            var insertTodoStatement = _session?.Prepare("INSERT INTO todos (id, task, completed, user_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)");

            foreach (var todo in todos)
            {
                var newGuid = Guid.NewGuid();
                uuids.Add(newGuid);
                todo.Id = newGuid; // Men-set UUID untuk Id
                todo.CreatedAt = DateTime.UtcNow; // Men-set timestamp saat ini
                todo.UpdatedAt = DateTime.UtcNow; // Men-set timestamp saat ini

                _session?.Execute(insertTodoStatement?.Bind(todo.Id, todo.Task, todo.Completed, todo.UserId, todo.CreatedAt, todo.UpdatedAt));
            }

            return uuids;
        }
        
        private static void UpdateTodo(Guid selectedUuid)
        {
            // Update data berdasarkan UUID yang dipilih
            // Misalnya update kolom task dari tabel todos berdasarkan UUID
            PreparedStatement? preparedStatement = _session?.Prepare("UPDATE todos SET task = ? WHERE id = ?");
            BoundStatement? boundStatement = preparedStatement?.Bind("Keep positive mind", selectedUuid);
            _session?.Execute(boundStatement);

            Console.WriteLine($"Updated task for UUID: {selectedUuid}");
        }

        private static void DeleteTodo(Guid selectedUuidToDelete)
        {
            // Menghapus data berdasarkan UUID yang dipilih
            var deleteStatement = _session?.Prepare("DELETE FROM todos WHERE id = ?");
            if (deleteStatement == null)
            {
                Console.WriteLine("Failed to prepare the delete statement.");
                return;
            }

            var boundStatement = deleteStatement.Bind(selectedUuidToDelete);
            _session?.Execute(boundStatement);

            Console.WriteLine($"Deleted task for UUID: {selectedUuidToDelete}");
        }

        public void Dispose()
        {
            _session?.Dispose();
            _cluster?.Dispose();
        }
    }
}
