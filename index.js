const express = require('express');
const mysql = require('mysql');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Connect to MySQL database
const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'your_password',
    database: 'your_database'
});

db.connect((err) => {
    if (err) throw err;
    console.log('Connected to the database.');
});

// Utility function to run queries using promises
const queryAsync = (sql, params) => {
    return new Promise((resolve, reject) => {
        db.query(sql, params, (err, results) => {
            if (err) return reject(err);
            resolve(results);
        });
    });
};

// Fetch game details by AppID
app.post('/get_game', async (req, res) => {
    const { AppID } = req.body;
    try {
        const query = 'SELECT * FROM steam_reviews WHERE AppID = ?';
        const results = await queryAsync(query, [AppID]);

        if (results.length === 0) {
            return res.status(404).send({ error: 'Game not found' });
        }
        res.send(results[0]);
    } catch (err) {
        res.status(500).send(err.message);
    }
});

// Update game details
app.post('/update_game', async (req, res) => {
    const { AppID, Reviews, ReviewType, Metacritic_url, Metacritic_score } = req.body;

    if (Metacritic_url === 'No metacritic URL' && Metacritic_score) {
        return res.status(400).send('Cannot update Metacritic score if URL is "No metacritic URL"');
    }

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

        await queryAsync(query, [Reviews, Metacritic_url, Metacritic_score, positiveIncrement, -negativeIncrement, AppID]);
        res.send('Game updated successfully');
    } catch (err) {
        res.status(500).send(err.message);
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
                return res.status(400).send('Invalid field selected');
        }

        await queryAsync(query, params);
        res.send('Field updated successfully');
    } catch (err) {
        res.status(500).send(err.message);
    }
});

// Start the server
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
