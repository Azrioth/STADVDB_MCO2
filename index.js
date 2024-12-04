const express = require('express');
const mysql = require('mysql2');
const path = require('path');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

//connect with databases
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


// node 2 connecting 
node2.connect((err) => {
    if (err) {
        console.error('Failed to connect to Node 2:', err.message);
        nodeHealth.node2 = false; 
    } else {
        console.log('Connected to Node 2.');
        nodeHealth.node2 = true; 
    }
});

// node 3 connecting 
node3.connect((err) => {
    if (err) {
        console.error('Failed to connect to Node 3:', err.message);
        nodeHealth.node3 = false; 
    } else {
        console.log('Connected to Node 3.');
        nodeHealth.node3 = true; 
    }
});

// central/master node connecting 
db.connect((err) => {
    if (err) {
        console.error('Failed to connect to Central Node:', err.message);
        nodeHealth.node1 = false; 
    } else {
        console.log('Connected to the central database.');
        nodeHealth.node1 = true; 
    }
});


// helper function for queries using promises
const queryAsync = (connection, sql, params) => {
    return new Promise((resolve, reject) => {
        connection.query(sql, params, (err, results) => {
            if (err) return reject(err);
            resolve(results);
        });
    });
};

// node health, is changed if not connected
const nodeHealth = {
    node1: true, 
    node2: true, 
    node3: true 
};

//function to check a node's health (if theyre connected)
const checkNodeHealth = async (connection, node) => {
    console.log("----------------------");
    try {
        await queryAsync(connection, 'SELECT 1'); //SELECT 1 just to see if it will respond, if responds then is connected
        nodeHealth[node] = true;
        console.log(`${node} is healthy.`); 
    } catch (err) {
        nodeHealth[node] = false;
        console.log(`${node} is not healthy: ${err.message}`);
    }
    
};

//health check every 10 seconds
setInterval(() => {
    checkNodeHealth(db, 'node1');
    checkNodeHealth(node2, 'node2');
    checkNodeHealth(node3, 'node3');
}, 10000);



//renders the index html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, '/public/index.html'));
});

//fetch all game details in database by AppID
app.post('/get_game', async (req, res) => {
    const { AppID } = req.body;

    try {
        let results;

        // if node 2 is healthy
        if (nodeHealth.node2) {
            try {
                //considering repeatable read, we need a "commit" line. 
                await queryAsync(node2, 'START TRANSACTION');
                results = await queryAsync(node2, 'SELECT * FROM mco2_ddbms_under2010 WHERE AppID = ?', [AppID]);
                await queryAsync(node2, 'COMMIT');

                //show the data that was received
                if (results.length > 0) {
                    console.log('Data retrieved from Node 2.');
                    return res.send({ source: 'Node 2', data: results[0] });
                }

            } catch (err) {
                console.error('Node 2 is down:', err.message);
                nodeHealth.node2 = false; //make nodehealth of 2 as unhealthy
            }
        }

        //if node 3 is healthy
        if (nodeHealth.node3) {
            try {
                //considering repeatable read, we need a "commit" line. 
                await queryAsync(node3, 'START TRANSACTION');
                results = await queryAsync(node3, 'SELECT * FROM mco2_ddbms_after2010 WHERE AppID = ?', [AppID]);
                await queryAsync(node3, 'COMMIT');

                //show the data that was received
                if (results.length > 0) {
                    console.log('Data retrieved from Node 3.');
                    return res.send({ source: 'Node 3', data: results[0] });
                }
            } catch (err) {
                console.error('Node 3 is down:', err.message);
                nodeHealth.node3 = false; //make nodehealth of 2 as unhealthy
            }
        }

        //fallback to central node
        if (nodeHealth.node1) {

            //considering two tables, a union select to get all elements 
            const allDataQuery = `
                SELECT * FROM mco2_ddbms_under2010 WHERE AppID = ?
                UNION
                SELECT * FROM mco2_ddbms_after2010 WHERE AppID = ?
                `;

            try {
                //considering repeatable read, we need a "commit" line. 
                await queryAsync(db, 'START TRANSACTION');
                results = await queryAsync(db, allDataQuery, [AppID, AppID]);
                await queryAsync(db, 'COMMIT');

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



// update game details
// app.post('/update_game', async (req, res) => {
//     const { AppID, Reviews, ReviewType, Metacritic_url, Metacritic_score, targetTable } = req.body;

//     const positiveIncrement = ReviewType === 'Positive' ? 1 : 0;
//     const negativeIncrement = ReviewType === 'Negative' ? 1 : 0;

//     const query = `
//         UPDATE ${targetTable}
//         SET Reviews = ?, Metacritic_url = ?, Metacritic_score = ?,
//             Positive_reviews = Positive_reviews + ?,
//             Negative_reviews = Negative_reviews + ?
//         WHERE AppID = ?
//     `;

//     try {
//         await queryAsync(db, query, [
//             Reviews, Metacritic_url, Metacritic_score, positiveIncrement, -negativeIncrement, AppID
//         ]);
//         res.send('Game updated successfully.');
//     } catch (err) {
//         console.error('Error updating game:', err.message);
//         res.status(500).send({ error: 'Failed to update game.' });
//     }
// });

//update game details
app.post('/update_game', async (req, res) => {
    const { AppID, Reviews, ReviewType, Metacritic_url, Metacritic_score, Release_date } = req.body;

    if (!Release_date) {
        return res.status(400).send('Release date is required for the update.');
    }
    // for adding later
    const positiveIncrement = ReviewType === 'Positive' ? 1 : 0;
    const negativeIncrement = ReviewType === 'Negative' ? 1 : 0;
    console.log('Received Release Date:', Release_date);

    //determine the correct table based on Release_date
    const targetTable = new Date(Release_date) < new Date('2010-01-01')
        ? 'mco2_ddbms_under2010'
        : 'mco2_ddbms_after2010';

    //update query
    const selectQuery = `SELECT * FROM ${targetTable} WHERE AppID = ?`;
    const updateQuery = `
        UPDATE ${targetTable}
        SET Reviews = ?, Metacritic_url = ?, Metacritic_score = ?,
            Positive_reviews = Positive_reviews + ?,
            Negative_reviews = Negative_reviews + ?
        WHERE AppID = ?
    `;
    const rollbackQuery = `
        UPDATE ${targetTable}
        SET Reviews = ?, Metacritic_url = ?, Metacritic_score = ?,
            Positive_reviews = ?, Negative_reviews = ?
        WHERE AppID = ?
    `;

      // Declare originalData in a higher scope for rollback purposes
    let originalData;

    try {
        // Begin transaction
        //checks if master is online
        const isDBOnline = await checkDatabaseConnection();
        if (!isDBOnline) {
            return res.status(500).send('Node 1 is offline. Transaction cancelled.');
        }

        //making the transaction serializable so updates like positive reviews will go sequentially.
        await queryAsync(db, 'SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE');
        await queryAsync(db, 'START TRANSACTION');

        //get first the original data (data in the centralnode)
        [originalData] = await queryAsync(db, selectQuery, [AppID]);

        //if cannot be found, then no record
        if (!originalData) {
            throw new Error('Record not found for the provided AppID.');
        }

        //checks for them if they are there
        if(targetTable == 'mco2_ddbms_under2010'){
            await queryAsync(node2, 'SELECT 1');
        }else{
            await queryAsync(node3, 'SELECT 1');
        }

        //update goes into master node
        await queryAsync(db, updateQuery, [
            Reviews, Metacritic_url, Metacritic_score,
            positiveIncrement, negativeIncrement, AppID
        ]);

        //commit query
        await queryAsync(db, 'COMMIT');
        res.send('Game updated successfully.');
    } catch (err) {
        console.error('Error updating game:', err.message);

        //should there be an error, rollback
        await queryAsync(db, 'ROLLBACK');

        //recover data
        try {
            if (originalData) {
                await queryAsync(db, rollbackQuery, [
                    originalData.Reviews,
                    originalData.Metacritic_url,
                    originalData.Metacritic_score,
                    originalData.Positive_reviews,
                    originalData.Negative_reviews,
                    originalData.AppID
                ]);
                console.log('Rollback recovery executed successfully.');
            }
        } catch (rollbackErr) {
            console.error('Rollback recovery failed:', rollbackErr.message);
        }

        res.status(500).send('Failed to update the game. Rollback executed.');
    }
});


// checks data base connection
async function checkDatabaseConnection() {
    try {
        // query if responsive
        const result = await queryAsync(db, 'SELECT 1');
        return result ? true : false;
    } catch (err) {
        console.error('Database connection check failed:', err.message);
        return false;
    }
}

app.get('/concurrent_read', async (req, res) => {
    const { AppID } = req.query; //gets app id from the query (that being input field)
    if (!AppID) {
        return res.status(400).send({ error: 'AppID is required' });
    }

    try {
        //unionized query
        const allDataQuery = `
        SELECT * FROM mco2_ddbms_under2010 WHERE AppID = ?
        UNION
        SELECT * FROM mco2_ddbms_after2010 WHERE AppID = ?
        `;
        //queries are in a promise
        const masterQuery = queryAsync(db, allDataQuery, [AppID, AppID])
            .then((data) => ({ node: 'Master', data }))
            .catch((err) => ({ node: 'Master', error: err.message }));

        const node2Query = nodeHealth.node2
            ? queryAsync(node2, 'SELECT * FROM mco2_ddbms.mco2_ddbms_under2010 WHERE AppID = ?', [AppID])
                .then((data) => ({ node: 'Node 2', data }))
                .catch((err) => ({ node: 'Node 2', error: err.message }))
            : Promise.resolve({ node: 'Node 2', error: 'Node is not available' });

        const node3Query = nodeHealth.node3
            ? queryAsync(node3, 'SELECT * FROM mco2_ddbms.mco2_ddbms_after2010 WHERE AppID = ?', [AppID])
                .then((data) => ({ node: 'Node 3', data }))
                .catch((err) => ({ node: 'Node 3', error: err.message }))
            : Promise.resolve({ node: 'Node 3', error: 'Node is not available' });

        //since theyre all promises, once run, they will concurrently happen
        const results = await Promise.all([masterQuery, node2Query, node3Query]);

        //format the results into a response object
        const response = {};
        results.forEach((result) => {
            if (result.error) {
                response[result.node] = result.error;
            } else if (result.data && result.data.length > 0) {
                response[result.node] = result.data;
            } else {
                response[result.node] = 'No Data Found';
            }
        });

        res.json(response);
    } catch (err) {
        res.status(500).send({ error: err.message });
    }
});


//insert a new game
app.post('/add_game', async (req, res) => {
    const { AppID, Game_Name, Release_date, Price, Required_age, Achievements } = req.body;

    try {
        //simulate a failure in the central node
        const simulateCentralNodeFailure = Math.random() < 0.50; // 50% chance of failure

        //query for inserting
        const query = `
            INSERT INTO gameinfo (AppID, Game_Name, Release_date, Price, Required_age, Achievements)
            VALUES (?, ?, ?, ?, ?, ?)
        `;

        //only master node allows insertions
        if (simulateCentralNodeFailure) {
            throw new Error('System failure: Central node is unavailable.');
        }
        await queryAsync(db, query, [AppID, Game_Name, Release_date, Price, Required_age, Achievements]);

        //to whom the data will replicate into
        const releaseDate = new Date(Release_date);
        if (releaseDate < new Date('2010-01-01')) {
            // Insert into Node 2
            await queryAsync(node2, query, [AppID, Game_Name, Release_date, Price, Required_age, Achievements]);
        } else {
            // Insert into Node 3
            await queryAsync(node3, query, [AppID, Game_Name, Release_date, Price, Required_age, Achievements]);
        }

        res.send('Game added successfully and replicated to the appropriate node.');
    } catch (err) {
        console.error('Error adding game:', err.message);

        //handle master failure
        if (err.message.includes('Central node is unavailable')) {
            res.status(500).send({
                message: 'Failed to add game. System Error.',
                error: err.message,
            });
        } else {
            res.status(500).send({
                message: 'Failed to add game.',
                error: err.message,
            });
        }
    }
});



app.get('/fetch_game/:AppID', async (req, res) => {
    const { AppID } = req.params;

    try {
        //check which node to query
        let nodeToQuery = null;

        if (nodeHealth.node2) {
            const pre2010Result = await queryAsync(node2, 'SELECT * FROM gameinfo WHERE AppID = ? AND Release_date < "2010-01-01"', [AppID]);
            if (pre2010Result.length > 0) {
                nodeToQuery = 'Node 2';
                return res.send({ source: nodeToQuery, data: pre2010Result[0] });
            }
        }

        if (nodeHealth.node3) {
            const post2010Result = await queryAsync(node3, 'SELECT * FROM gameinfo WHERE AppID = ? AND Release_date >= "2010-01-01"', [AppID]);
            if (post2010Result.length > 0) {
                nodeToQuery = 'Node 3';
                return res.send({ source: nodeToQuery, data: post2010Result[0] });
            }
        }

        //fallback to master if slaves are down in getting game info
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

//delete a specific field, not data
app.post('/delete_field', async (req, res) => {
    const { AppID, Field } = req.body; //get app id in input

    const targetTable = new Date(Release_date) < new Date('2010-01-01')
        ? 'mco2_ddbms_under2010'
        : 'mco2_ddbms_after2010';
    
    try {
        //query + params since deleting is just updating without data.
        let query;
        let params;
        switch (Field) {
            case 'Reviews':
                query = `UPDATE ${targetTable} SET Reviews = ? WHERE AppID = ?`;
                params = ['No review', AppID];
                break;
            case 'Metacritic_score':
                query = `UPDATE ${targetTable} SET Metacritic_score = ? WHERE AppID = ?`;
                params = [0.0, AppID];
                break;
            case 'Metacritic_url':
                query = `UPDATE ${targetTable} SET Metacritic_url = ?, Metacritic_score = ? WHERE AppID = ?`;
                params = ['No metacritic URL', 0.0, AppID];
                break;
            default:
                return res.status(400).send('Invalid field selected.');
        }
        await queryAsync(db, 'SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE');
        await queryAsync(db, 'START TRANSACTION');
        await queryAsync(db, query);

        await queryAsync(db, 'COMMIT');

        res.send('Field updated successfully.');
    } catch (err) {
        console.error('Error deleting field: ', err.message)
        await queryAsync(db, 'ROLLBACK');
        res.status(500).send('Failed to delete field');
    }
});

//report for review summary
app.get('/fetch_reviews_summary', async (req, res) => {
    try {
        const query = `
            SELECT 
                YEAR(Release_date) AS Year, 
                SUM(Positive_reviews) AS Positive_reviews, 
                SUM(Negative_reviews) AS Negative_reviews, 
                CASE 
                    WHEN SUM(Negative_reviews) = 0 THEN NULL
                    ELSE ROUND(SUM(Positive_reviews) / (SUM(Positive_reviews)+SUM(Negative_reviews)), 2) 
                END AS Customer_satisfaction
            FROM (
                SELECT Release_date, Positive_reviews, Negative_reviews FROM mco2_ddbms_after2010
                UNION ALL
                SELECT Release_date, Positive_reviews, Negative_reviews FROM mco2_ddbms_under2010
            ) AS CombinedData
            GROUP BY YEAR(Release_date)
            ORDER BY Year;
        `;

        const results = await queryAsync(db, query);

        //results will show in html
        res.json(results);
    } catch (err) {
        console.error("Error fetching review summary:", err);
        res.status(500).send({ error: 'Failed to fetch review summary' });
    }
});

//server start
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
