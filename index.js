const express = require('express');
const bodyParser = require('body-parser');
const csv = require('csv-parser');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;
console.log(`port is running in port number ${port}`)

// Configure environment variables
const csvFilePath = process.env.CSV_FILE_PATH || '/path/to/csv/file.csv';
const dbConfig = {
  user: process.env.DB_USER || 'user',
  password: process.env.DB_PASSWORD || 'password',
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5000,
  database: process.env.DB_NAME || 'database',
};

// Create a PostgreSQL client pool
const pool = new Pool(dbConfig);

// Defining the API endpoint for uploading CSV data
app.post('/upload', bodyParser.raw({ type: 'text/csv' }), async (request, response) => {
  const results = [];
  request
    .pipe(csv())
    .on('data', (data) => {
      // Map the mandatory properties to designated fields of the PostgreSQL DB table
      const user = {
        name: `${data['name.firstName']} ${data['name.lastName']}`,
        age: data.age,
        address: {
          line1: data['address.line1'],
          line2: data['address.line2'],
          city: data['address.city'],
          state: data['address.state'],
        },
        additional_info: {},
      };

      // Put the remaining properties to additional_info field as a JSON object
      Object.keys(data).forEach((key) => {
        if (
          key !== 'name.firstName' &&
          key !== 'name.lastName' &&
          key !== 'age' &&
          key !== 'address.line1' &&
          key !== 'address.line2' &&
          key !== 'address.city' &&
          key !== 'address.state'
        ) {
          user.additional_info[key] = data[key];
        }
      });

      results.push(user);
    })
    .on('end', async () => {
      // Connect to the PostgreSQL DB and insert the data into the users table
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const insertQueries = results.map(
          (user) =>
            `INSERT INTO users (name, age, address, additional_info) VALUES ('${user.name}', ${user.age}, '${JSON.stringify(
              user.address,
            )}', '${JSON.stringify(user.additional_info)}')`,
        );
        await Promise.all(insertQueries.map((q) => client.query(q)));
        await client.query('COMMIT');
        console.log("user",user);
        response.status(200).json({
            status:true,
            msg:"data is updated successfully:: "
        });
      } catch (err) {
        await client.query('ROLLBACK');
        console.error(err)
    }})
})
