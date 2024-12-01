const express = require('express');
const mysql = require('mysql2');
const path = require('path');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Connect to MySQL database
const db = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    port: 20662,
    user: 'remote',
    password: 'remotepassword',
    database: 'mco2_ddbms',
    ssl: { rejectUnauthorized: false }
});

const node2 = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    port: 20672,
    user: 'remote',
    password: 'remotepassword',
    database: 'mco2_ddbms_under2010',
    ssl: { rejectUnauthorized: false }
});

const node3 = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    port: 20682,
    user: 'remote',
    password: 'remotepassword',
    database: 'mco2_ddbms_after2010',
    ssl: { rejectUnauthorized: false }
});

db.connect((err) => {
    if (err) throw err;
    console.log('Connected to the central database.');
});

node2.connect((err) => {
    if (err) throw err;
    console.log('Connected to Node 2.');
});

node3.connect((err) => {
    if (err) throw err;
    console.log('Connected to Node 3.');
});

// Utility function to run queries using promises
const queryAsync = (connection, sql, params) => {
    return new Promise((resolve, reject) => {
        connection.query(sql, params, (err, results) => {
            if (err) return reject(err);
            resolve(results);
        });
    });
};

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '/public/index.html'));
});

// Fetch game details by AppID
app.post('/get_game', async (req, res) => {
    const { AppID } = req.body;

    try {
        let results;

        // Check Node 2 first
        results = await queryAsync(node2, 'SELECT * FROM gameinfo WHERE AppID = ?', [AppID]);
        if (results.length > 0) {
            console.log('Data retrieved from Node 2.');
            return res.send({ source: 'Node 2', data: results[0] });
        }

        // Check Node 3 next
        results = await queryAsync(node3, 'SELECT * FROM gameinfo WHERE AppID = ?', [AppID]);
        if (results.length > 0) {
            console.log('Data retrieved from Node 3.');
            return res.send({ source: 'Node 3', data: results[0] });
        }

        // Check Central Node as a fallback
        results = await queryAsync(db, 'SELECT * FROM gameinfo WHERE AppID = ?', [AppID]);
        if (results.length > 0) {
            console.log('Data retrieved from Central Node (Node 1).');
            return res.send({ source: 'Central Node', data: results[0] });
        }

        res.status(404).send({ error: 'Game not found.' });
    } catch (err) {
        res.status(500).send({ error: err.message });
    }
});

// Update game details
app.post('/update_game', async (req, res) => {
    const { AppID, Reviews, ReviewType, Metacritic_url, Metacritic_score } = req.body;

    try {
        const positiveIncrement = ReviewType === 'Positive' ? 1 : 0;
        const negativeIncrement = ReviewType === 'Negative' ? 1 : 0;

        const query = `
            UPDATE steam_reviews
            SET Reviews = ?, Metacritic_url = ?, Metacritic_score = ?,
                Positive_reviews = Positive_reviews + ?,
                Negative_reviews = Negative_reviews + ?
            WHERE AppID = ?
        `;

        await queryAsync(db, query, [
            Reviews, Metacritic_url, Metacritic_score, positiveIncrement, -negativeIncrement, AppID
        ]);
        res.send('Game updated successfully.');
    } catch (err) {
        res.status(500).send({ error: err.message });
    }
});

// Delete specific review data
app.post('/delete_field', async (req, res) => {
    const { AppID, Field } = req.body;

    try {
        let query;
        let params;

        switch (Field) {
            case 'Reviews':
                query = 'UPDATE steam_reviews SET Reviews = ? WHERE AppID = ?';
                params = ['No review', AppID];
                break;
            case 'Metacritic_score':
                query = 'UPDATE steam_reviews SET Metacritic_score = ? WHERE AppID = ?';
                params = [0.0, AppID];
                break;
            case 'Metacritic_url':
                query = `
                    UPDATE steam_reviews
                    SET Metacritic_url = ?, Metacritic_score = ?
                    WHERE AppID = ?
                `;
                params = ['No metacritic URL', 0.0, AppID];
                break;
            default:
                return res.status(400).send('Invalid field selected.');
        }

        await queryAsync(db, query, params);
        res.send('Field updated successfully.');
    } catch (err) {
        res.status(500).send({ error: err.message });
    }
});

// Start the server
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
