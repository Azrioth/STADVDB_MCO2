const express = require('express');
const mysql = require('mysql2');
const app = express();
const port = 3000;

// Create MySQL connection
const connection = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    user: 'remote',
    password: 'remotepassword',
    database: 'mco2_ddbms'
});

const connection2 = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    user: 'remote',
    password: 'remotepassword',
    database: 'mco2_ddbms_under2010'
});

const connection3 = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    user: 'remote',
    password: 'remotepassword',
    database: 'mco2_ddbms_after2010'
});

let isConnected1 = mysql.createPool({...connection, waitForConnections: true, connectionLimit: 4});
let isConnected2 = mysql.createPool({...connection, waitForConnections: true, connectionLimit: 4});
let isConnected3 = mysql.createPool({...connection, waitForConnections: true, connectionLimit: 4});

// Middleware to parse incoming JSON requests
app.use(express.json());

// Serve static files (HTML, JS, CSS) from the "public" folder
app.use(express.static('public'));

async function monitorConnections() {
    try {
        // Check connection for DB1
        await isConnected1.query('SELECT 1');
        connected1 = true;
    } catch (err) {
        console.error('Node 1 Connection Error:', err.message);
        connected1 = false;
    }

    try {
        // Check connection for DB2
        await isConnected2.query('SELECT 1');
        connected2 = true;
    } catch (err) {
        console.error('Node 2 Connection Error:', err.message);
        connected2 = false;
    }

    try {
        // Check connection for DB3
        await isConnected3.query('SELECT 1');
        connected3 = true;
    } catch (err) {
        console.error('Node 3 Connection Error:', err.message);
        connected3 = false;
    }
}

//check connection status every 5 seconds, can be changed if desired/needed
setInterval(monitorConnections, 5000);

//Gets database connection status
app.get('/connection_status', (req, res) => {
    res.json({
        isConnected1: connected1,
        isConnected2: connected2,
        isConnected3: connected3
    });

});

// Route to fetch game details by AppID
app.get('/get_game_details/:appID', async (req, res) => {
    const { appID } = req.params;
    try {
        const [rows] = await connection.promise().query('SELECT * FROM dim_feedback WHERE AppID = ?', [appID]);
        if (rows.length === 0) {
            return res.status(404).send('Game not found');
        }
        res.json(rows[0]);
    } catch (err) {
        res.status(500).send('Error retrieving game details: ' + err.message);
    }
});

// Route to update game details
app.post('/update_game', async (req, res) => {
    const { AppID, Reviews, Metacritic_url, Metacritic_score, ReviewType } = req.body;
    let updateQuery = 'UPDATE dim_feedback SET ';
    let values = [];

    if (Reviews) {
        updateQuery += 'Reviews = ?, ';
        values.push(Reviews);
    }
    if (Metacritic_url) {
        updateQuery += 'Metacritic_url = ?, ';
        values.push(Metacritic_url);
    }
    if (Metacritic_score) {
        updateQuery += 'Metacritic_score = ?, ';
        values.push(Metacritic_score);
    }
    if (ReviewType) {
        if (ReviewType === 'Positive') {
            updateQuery += 'Positive_reviews = Positive_reviews + 1, Negative_reviews = Negative_reviews - 1 ';
        } else if (ReviewType === 'Negative') {
            updateQuery += 'Negative_reviews = Negative_reviews + 1, Positive_reviews = Positive_reviews - 1 ';
        }
    }

    updateQuery += 'WHERE AppID = ?';
    values.push(AppID);

    try {
        await connection.promise().query(updateQuery, values);
        const [updatedRows] = await connection.promise().query('SELECT * FROM dim_feedback WHERE AppID = ?', [AppID]);
        res.json({ message: 'Game updated successfully', game: updatedRows[0] });
    } catch (err) {
        res.status(500).json({ message: 'Error updating game details', error: err.message });
    }
});

// Route to delete specific game details
app.post('/delete_review', async (req, res) => {
    const { AppID, fieldToDelete } = req.body;
    let updateQuery;
    let values = [];

    switch (fieldToDelete) {
        case 'Reviews':
            updateQuery = 'UPDATE dim_feedback SET Reviews = "No review" WHERE AppID = ?';
            break;
        case 'Metacritic_score':
            updateQuery = 'UPDATE dim_feedback SET Metacritic_score = 0.0 WHERE AppID = ?';
            break;
        case 'Metacritic_url':
            updateQuery = 'UPDATE dim_feedback SET Metacritic_url = "No metacritic URL", Metacritic_score = 0.0 WHERE AppID = ?';
            break;
        default:
            return res.status(400).json({ message: 'Invalid field to delete' });
    }
    
    values.push(AppID);
    
    try {
        await connection.promise().query(updateQuery, values);
        const [updatedRows] = await connection.promise().query('SELECT * FROM dim_feedback WHERE AppID = ?', [AppID]);
        res.json({ message: 'Field deleted successfully', game: updatedRows[0] });
    } catch (err) {
        res.status(500).json({ message: 'Error deleting field', error: err.message });
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});
