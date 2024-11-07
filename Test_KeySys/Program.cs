using System;
using Npgsql;
using System.IO;
using Microsoft.Extensions.Configuration;

class Program
{
	static void Main(string[] args)
	{
		var configuration = new ConfigurationBuilder()
			.SetBasePath(Directory.GetCurrentDirectory())
			.AddJsonFile("appsettings.json")
			.Build();

		string connection = configuration.GetConnectionString("DefaultConnection");

		using (var connect = new NpgsqlConnection(connection))
		{
			connect.Open();

			// Запрос 1
			Console.WriteLine("Запрос 1:");
			string query1 = @"
                SELECT e.first_name, e.last_name, e.email
                FROM employees e
                WHERE e.manager_id IN (
                    SELECT employee_id 
                    FROM employees 
                    WHERE hire_date >= '2023-01-01'
                ) AND e.hire_date < '2023-01-01';
            ";
			ExecuteQuery(query1, connect);

			// Запрос 2
			Console.WriteLine("Запрос 2:");
			string query2 = @"
                SELECT e.first_name, j.job_title, d.department_name
                FROM employees e
                JOIN jobs j ON e.job_id = j.job_id
                JOIN departments d ON e.department_id = d.department_id;
            ";
			ExecuteQuery(query2, connect);

			// Запрос 3
			Console.WriteLine("Запрос 3:");
			string query3 = @"
                SELECT l.city
				FROM locations l
				JOIN employees e ON e.department_id = l.location_id
				JOIN countries c ON l.country_id = c.country_id
				GROUP BY l.city
				ORDER BY SUM(e.salary) ASC
				LIMIT 1;";
			ExecuteQuery(query3, connect);

			// Запрос 4
			Console.WriteLine("Запрос 4:");
			string query4 = @"
                SELECT e.first_name, e.last_name, e.email, e.job_id, e.salary
				FROM employees e
				JOIN jobs j ON e.job_id = j.job_id
				WHERE e.manager_id IN (
    				SELECT employee_id 
					FROM employees 
					WHERE EXTRACT(MONTH FROM hire_date) = 1
				) AND LENGTH(j.job_title) > 15;
            ";
			ExecuteQuery(query4, connect);

			// Запрос 5
			Console.WriteLine("Запрос 5:");
			string query5 = @"
                SELECT l.city
				FROM locations l
				JOIN employees e ON e.department_id = l.location_id
				JOIN countries c ON l.country_id = c.country_id
				GROUP BY l.city
				ORDER BY SUM(e.salary) ASC
				LIMIT 1;
            ";
			ExecuteQuery(query5, connect);

			// Запрос 6
			Console.WriteLine("Запрос 6:");
			string query6 = @"
                SELECT e.first_name, e.last_name, d.department_name, j.job_title, l.street_address, c.country_name, r.region_name
				FROM employees e
				JOIN departments d ON e.department_id = d.department_id
				JOIN jobs j ON e.job_id = j.job_id
				JOIN locations l ON d.location_id = l.location_id
				JOIN countries c ON l.country_id = c.country_id
				JOIN regions r ON c.region_id = r.region_id;

            ";
			ExecuteQuery(query6, connect);

			// Запрос 7
			Console.WriteLine("Запрос 7:");
			string query7 = @"
                SELECT r.region_name, COUNT(e.employee_id) AS employee_count
				FROM regions r
				JOIN countries c ON r.region_id = c.region_id
				JOIN locations l ON c.country_id = l.country_id
				JOIN departments d ON l.location_id = d.location_id  
				JOIN employees e ON e.department_id = d.department_id 
				GROUP BY r.region_name;
            ";
			ExecuteQuery(query7, connect);

			// Запрос 8
			Console.WriteLine("Запрос 8:");
			string query8 = @"
                SELECT d.department_name, 
                       MIN(e.salary) AS min_salary, 
                       MAX(e.salary) AS max_salary,
                       MIN(e.hire_date) AS earliest_hire,
                       MAX(e.hire_date) AS latest_hire,
                       COUNT(e.employee_id) AS employee_count
                FROM employees e
                JOIN departments d ON e.department_id = d.department_id
                GROUP BY d.department_name
                ORDER BY employee_count DESC;
            ";
			ExecuteQuery(query8, connect);

			// Запрос 9
			Console.WriteLine("Запрос 9:");
			string query9 = @"
                SELECT e.first_name, e.last_name, SUBSTRING(e.phone_number, 1, 3) AS phone_prefix
                FROM employees e
                WHERE e.phone_number ~ '^\d{3}\.\d{3}\.\d{4}$';
            ";
			ExecuteQuery(query9, connect);

			// Запрос 10
			Console.WriteLine("Запрос 10:");
			string query10 = @"
                SELECT e.first_name, e.last_name
                FROM employees e
                JOIN departments d ON e.department_id = d.department_id
                WHERE d.department_name = 'DAD';
            ";
			ExecuteQuery(query10, connect);

			connect.Close();
		}
	}

	// Метод для выполнения SQL-запросов
	static void ExecuteQuery(string query, NpgsqlConnection connection)
	{
		using (var a = new NpgsqlCommand(query, connection))
		using (var b = a.ExecuteReader())
		{
			bool c = b.HasRows;
			if (c)
			{
				for (int i = 0; i < b.FieldCount; i++)
				{
					Console.Write(b.GetName(i) + "\t");
				}
				Console.WriteLine();

				while (b.Read())
				{
					for (int i = 0; i < b.FieldCount; i++)
					{
						Console.Write(b.GetValue(i) + "\t");
					}
					Console.WriteLine();
				}
			}
			else
			{
				Console.WriteLine("Нет данных!!!");
			}
		}
	}
}

