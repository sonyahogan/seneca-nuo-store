var nuodb = require('db-nuodb');
    new nuodb.Database({
        hostname: 'localhost',
        user: 'dba',
        password: 'goalie',
        database: 'test',
        schema: 'HOCKEY'
    }).connect(function(error) {
        if (error) {
            return console.log('CONNECTION error: ' + error);
        }
        this.query().
            select('*').
            from('players').
            where('weight > 200').
            execute(function(error, rows, cols) {
                    if (error) {
                            console.log('ERROR: ' + error);
                            return;
                    }
                    console.log(rows.length + ' ROWS found');
            });
    });