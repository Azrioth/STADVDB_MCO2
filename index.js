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
    database: 'mco2_ddbms',
    ssl: { rejectUnauthorized: false }
});

const node3 = mysql.createConnection({
    host: 'ccscloud.dlsu.edu.ph',
    port: 20682,
    user: 'remote',
    password: 'remotepassword',
    database: 'mco2_ddbms',
    ssl: { rejectUnauthorized: false }
});


// db.connect((err) => {
//     if (err) throw err;
//     console.log('Connected to the central database.');
// });

// node2.connect((err) => {
//     if (err) throw err;
//     console.log('Connected to Node 2.');
// });

// node3.connect((err) => {
//     if (err) throw err;
//     console.log('Connected to Node 3.');
// });



node2.connect((err) => {
    if (err) {
        console.error('Failed to connect to Node 2:', err.message);
        nodeHealth.node2 = false; // Mark Node 2 as down
    } else {
        console.log('Connected to Node 2.');
        nodeHealth.node2 = true; // Mark Node 2 as healthy
    }
});

node3.connect((err) => {
    if (err) {
        console.error('Failed to connect to Node 3:', err.message);
        nodeHealth.node3 = false; // Mark Node 3 as down
    } else {
        console.log('Connected to Node 3.');
        nodeHealth.node3 = true; // Mark Node 3 as healthy
    }
});

db.connect((err) => {
    if (err) {
        console.error('Failed to connect to Central Node:', err.message);
        nodeHealth.node1 = false; // Mark Central Node as down
    } else {
        console.log('Connected to the central database.');
        nodeHealth.node1 = true; // Mark Central Node as healthy
    }
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


const nodeHealth = {
    node1: true, // Represents the central database
    node2: true, // Represents the node for pre-2010 games
    node3: true  // Represents the node for post-2010 games
};

const checkNodeHealth = async (connection, node) => {
    console.log("----------------------");
    try {
        await queryAsync(connection, 'SELECT 1'); // Simple query to check connectivity
        nodeHealth[node] = true;
        console.log(`${node} is healthy.`);
    } catch (err) {
        nodeHealth[node] = false;
        console.log(`${node} is not healthy: ${err.message}`);
    }
    
};

// Periodic health checks every minute
setInterval(() => {
    checkNodeHealth(db, 'node1');
    checkNodeHealth(node2, 'node2');
    checkNodeHealth(node3, 'node3');
}, 10000);




app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '/public/index.html'));
});

// Fetch game details by AppID
app.post('/get_game', async (req, res) => {
    const { AppID } = req.body;

    try {
        let results;

        // Check Node 2 first if healthy
        if (nodeHealth.node2) {
            try {
                results = await queryAsync(node2, 'SELECT * FROM mco2_ddbms_under2010 WHERE AppID = ?', [AppID]);
                if (results.length > 0) {
                    console.log('Data retrieved from Node 2.');
                    return res.send({ source: 'Node 2', data: results[0] });
                }
            } catch (err) {
                console.error('Node 2 is down:', err.message);
                nodeHealth.node2 = false; // Mark Node 2 as down
            }
        }

        // Check Node 3 if healthy
        if (nodeHealth.node3) {
            try {
                results = await queryAsync(node3, 'SELECT * FROM mco2_ddbms_after2010 WHERE AppID = ?', [AppID]);
                if (results.length > 0) {
                    console.log('Data retrieved from Node 3.');
                    return res.send({ source: 'Node 3', data: results[0] });
                }
            } catch (err) {
                console.error('Node 3 is down:', err.message);
                nodeHealth.node3 = false; // Mark Node 3 as down
            }
        }

        // Fallback to Central Node
        if (nodeHealth.node1) {

            const allDataQuery = `
                SELECT * FROM mco2_ddbms_under2010 WHERE AppID = ?
                UNION
                SELECT * FROM mco2_ddbms_after2010 WHERE AppID = ?
                `;

            try {
                results = await queryAsync(db, allDataQuery, [AppID, AppID]);
                
                if (results.length > 0) {
                    console.log('Data retrieved from Master Node.');
                    return res.send({ source: 'Central Node', data: results[0] });
                }
            } catch (err) {
                console.error('Central Node is down:', err.message);
                nodeHealth.node1 = false; // Mark Central Node as down
            }
        }
        console.log("--------------------------");
        // If no data found
        res.status(404).send({ error: 'Game not found.' });
    } catch (err) {
        res.status(500).send({ error: 'Internal server error: ' + err.message });
    }
});



// Update game details
app.post('/update_game', async (req, res) => {
    const { AppID, Reviews, ReviewType, Metacritic_url, Metacritic_score } = req.body;

    try {
        const positiveIncrement = ReviewType === 'Positive' ? 1 : 0;
        const negativeIncrement = ReviewType === 'Negative' ? 1 : 0;

        const query = `
            UPDATE gameinfo
            SET Reviews = ?, Metacritic_url = ?, Metacritic_score = ?,
                Positive_reviews = Positive_reviews + ?,
                Negative_reviews = Negative_reviews + ?
            WHERE AppID = ?
        `;

        // Update in the master node
        await queryAsync(db, query, [
            Reviews, Metacritic_url, Metacritic_score, positiveIncrement, -negativeIncrement, AppID
        ]);

        // Propagate the update to Node 2 (pre-2010)
        try {
            await queryAsync(node2, query, [
                Reviews, Metacritic_url, Metacritic_score, positiveIncrement, -negativeIncrement, AppID
            ]);
        } catch (err) {
            console.error('Failed to propagate update to Node 2:', err.message);
        }

        // Propagate the update to Node 3 (post-2010)
        try {
            await queryAsync(node3, query, [
                Reviews, Metacritic_url, Metacritic_score, positiveIncrement, -negativeIncrement, AppID
            ]);
        } catch (err) {
            console.error('Failed to propagate update to Node 3:', err.message);
        }

        res.send('Game updated successfully.');
    } catch (err) {
        res.status(500).send({ error: err.message });
    }
});


// Insert a new game
app.post('/add_game', async (req, res) => {
    const { AppID, Game_Name, Release_date, Price, Required_age, Achievements } = req.body;

    try {
        // Insert into the master node
        const query = `
            INSERT INTO gameinfo (AppID, Game_Name, Release_date, Price, Required_age, Achievements)
            VALUES (?, ?, ?, ?, ?, ?)
        `;
        await queryAsync(db, query, [AppID, Game_Name, Release_date, Price, Required_age, Achievements]);

        // Determine which node to replicate the data to
        const releaseDate = new Date(Release_date);
        if (releaseDate < new Date('2010-01-01')) {
            // Insert into Node 2
            await queryAsync(node2, query, [AppID, Game_Name, Release_date, Price, Required_age, Achievements]);
        } else {
            // Insert into Node 3
            await queryAsync(node3, query, [AppID, Game_Name, Release_date, Price, Required_age, Achievements]);
        }

        res.send('Game added successfully and replicated to appropriate node.');
    } catch (err) {
        res.status(500).send({ error: err.message });
    }});
app.get('/fetch_game/:AppID', async (req, res) => {
    const { AppID } = req.params;

    try {
        // Check which node to query
        let nodeToQuery = null;

        if (nodeHealth.node2) {
            // Pre-2010 logic
            const pre2010Result = await queryAsync(node2, 'SELECT * FROM gameinfo WHERE AppID = ? AND Release_date < "2010-01-01"', [AppID]);
            if (pre2010Result.length > 0) {
                nodeToQuery = 'Node 2';
                return res.send({ source: nodeToQuery, data: pre2010Result[0] });
            }
        }

        if (nodeHealth.node3) {
            // Post-2010 logic
            const post2010Result = await queryAsync(node3, 'SELECT * FROM gameinfo WHERE AppID = ? AND Release_date >= "2010-01-01"', [AppID]);
            if (post2010Result.length > 0) {
                nodeToQuery = 'Node 3';
                return res.send({ source: nodeToQuery, data: post2010Result[0] });
            }
        }

        // Fallback to master node if slaves are down
        const masterResult = await queryAsync(db, 'SELECT * FROM gameinfo WHERE AppID = ?', [AppID]);
        if (masterResult.length > 0) {
            nodeToQuery = 'Master Node';
            return res.send({ source: nodeToQuery, data: masterResult[0] });
        }

        res.status(404).send('Game not found.');
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
