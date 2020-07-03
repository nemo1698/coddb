// CoDB v1.1
//===============================================
// Release notes
// 1.1
//
// bugfix: file in use fixed ( if the dbfile in use, the operation added to queue )
// ______________________________________________
// 1.0
//
// +load function
// +reload function
// +validate function
// +save function
// +db_select function
// +db_insert function
// +db_update function
// +db_delete function
// +db_assocc function
// +db_concat function
// +dumpdb function
// +dumpdbs function
// +debug function
// ______________________________________________

/**
* Loads a database and returns it id
*/
load( dbname )
{
	
	id = -1;
	
	db = _load( dbname );
	
	if( isDefined( db ) )
	{
		if( !isDefined( level._db ) )
		{
			level._db = [];
		}
		
		for(i=0; i<level._db.size; i++)
		{
			// db already loaded
			if( level._db[ i ].name == dbname )
			{
				debug( "DB Already Loaded " + dbname + " (" + i + ")" );
				return i;
			}
		}
		
		id = level._db.size;
		
		db.dbid = id;
		
		level._db[ id ] = db;
		
		
	}
	
	debug( "Loading db " + id );
	
	return id;
	
}

/**
* Load the db without put again into the array
*/
reload( db )
{
	if( db == -1 )
	{
		return false;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		debug( "Reloading DB: " + db.name + " (" + db.dbid + ")" );
		level._db[ db.dbid ] = _load( db.name );
		return true;
	}
	
	debug( "Failed to reload DB" );
	return false;
}

/**
*
*/
validate( db )
{
	debug("Validating database");
	
	if( db == -1 )
	{
		debug("DataBase not exists");
		return false;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		
		debug("Loading database");
		
		mdb = _load( db.name );
		
		debug("Database loaded");
		
		if( !isDefined( mdb ) )
		{
			debug("Failed to load db");
			return false;
		}
		
		if( db.keys.size != mdb.keys.size)
		{
			debug("Keys size not match");
			return false;
		}
		
		if( db.rows.size != mdb.rows.size )
		{
			debug("Rows size not match (" + db.rows.size + " " + mdb.rows.size + ")" );
			dumpdb( mdb );
			return false;
		}
		
		if( db.name != mdb.name )
		{
			debug("Database names not match");
			return false;
		}
		
		if( db.file != mdb.file )
		{
			debug("Different file");
			return false;
		}
		
		if( db.ai != mdb.ai )
		{
			debug("Auto increment not match");
			return false;
		}
		
		if( db.aikey != mdb.aikey )
		{
			debug("Auto increment key not match");
			return false;
		}
		
		debug("Meta data was valid");
		
		for( i=0; i<db.keys.size; i++ )
		{
			if( db.keys[i] != mdb.keys[i] )
				return false;
		}
		
		debug("Keys data was valid");
		
		for( i=0; i<db.rows.size; i++ )
		{
			for( j=0; j<db.rows[ i ].size; j++ )
			{
				if( db.rows[ i ][ j ] != mdb.rows[ i ][ j ] )
					return false;
			}
		}
		
		debug("Database was valid");
		return true;
		
	}
	
	debug("not found");
	
	return false;
}

_load( dbname )
{
	
	if( !isDefined( level.workingdir ) )
		level.workingdir = getCvar("fs_basepath") + "/main/";
	
	filename = level.workingdir + dbname + ".dat";
	
	debug( "Starting load DB" );

	if( fexists( filename ) )
	{
		debug("Opening db file");
		f = fopen( filename, "r+" );
		debug("Reading data");
		data = fread( 0, f );
		debug("Data read from db");
		fclose(f);
		
		if( isDefined( data ) )
		{
			debug("Parse Data");
			data = codam\utils::splitArray(data, "\n");
			firstRow = codam\utils::splitArray(data[0], "%");
			
			keys = [];
			
			if(firstRow.size-2 < 1)
				return -1;
			
			for(i=0; i<firstRow.size-2; i++)
			{
				keys[i] = firstRow[i];
			}
			
			db = spawnStruct();
			db.name = dbname;
			db.file = filename;
			db.keys = keys;
			db.ai = (int)firstRow[firstRow.size - 1];
			db.aikey = firstRow[firstRow.size - 2];
			db.queueId = 0;
			db.queue = [];
			db.queueResult = [];
			db.queue_active = false;
			db.inuse = false;
			db.rows = [];
			
			
			for(i=1; i<data.size; i++)
			{
				row = codam\utils::splitArray(data[i], "%");
				if( row.size == db.keys.size )
					db.rows[db.rows.size] = row;
			}
			
			return db;
			
		}
	}
	
	return undefined;
}

save( db )
{
	
	if( db == -1 )
	{
		debug( "Invalid DB" );
		return false;
	}

	debug( "Save requst on " + db );
	
	db = level._db[ db ];
	
	if( isDefined( db ) && isDefined( db.file ) && fexists( db.file ) )
	{
		debug( "Saving DB" );

		data = db.keys[ 0 ];
		for( i=1; i<db.keys.size; i++ )
		{
			data += "%%" + db.keys[ i ];
		}
		
		data += "%%" + db.aikey + "%%" + db.ai + "\n";
		
		for( i=0; i<db.rows.size; i++ )
		{
			row = db.rows[ i ];
			
			if( !isDefined( row ) )
				continue;
			
			data += row[0];
			if( isDefined( row[1] ) )
			{
				for( j=1; j<row.size; j++ )
				{
					if( isDefined( row[ j ] ) )
						data += "%%" + row[ j ];
				}
			}
			data += "\n";
		}
		debug( "Opening the DB file to write" );

		db.inuse = true;

		f = fopen( db.file, "w" );
		if( f == -1 )
		{
			db.inuse = false;
			return false;
		}
		
		debug( "Writing data to DB" );
		fwrite( data, f );
		debug( "Data wrote to DB" );
		fclose( f );
		
		debug( "saved" );

		db.inuse = false;

		return true;
	}
	
	debug( "Failed to Save" );
	return false;
}

db_select( db, key, sign, val )
{
	result = [];
	
	if( db == -1 )
	{
		return result;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		index = -1;
		for( i=0; i<db.keys.size; i++ )
		{
			if( db.keys[ i ] == key )
			{
				index = i;
				break;
			}
		}
		
		if( index == -1 && key != "*" )
		{
			debug( "Invalid key " + key );
			return result;
		}
		
		for( i=0; i<db.rows.size; i++ )
		{
			if( key == "*" )
				result[ result.size ] = db.rows[ i ];
			else if( isDefined( db.rows[ i ][ index ] ) && _if( db.rows[ i ][ index ], sign, val ) )
				result[ result.size ] = db.rows[ i ];
		}
	}
	
	debug( "Select executed with " + result.size + " records" );
	
	return db_assoc( db.dbid, result );
}

db_insert( db, row )
{
	if( db == -1 )
	{
		debug( "Invalid database" );
		return false;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		newRow = [];
		
		for( i=0; i<db.keys.size; i++ )
		{
			if( db.aikey != "None" && db.keys[ i ] == db.aikey )
			{
				db.ai++;
				newRow[ i ] = db.ai;
			}
			else
			{
				newRow[ i ] = row[ db.keys[ i ] ];
			}
		}
		
		db.rows[ db.rows.size ] = newRow;
		
		debug( "1 row inserted" );
		
		return save( db.dbid );
		
	}
	
	debug( "Cannot find db" );
	
	return false;
}

db_update( db, key, sign, val, dict )
{
	if( db == -1 )
	{
		return false;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		index = -1;
		for( i=0; i<db.keys.size; i++ )
		{
			if( db.keys[ i ] == key )
			{
				index = i;
				break;
			}
		}
		
		
		for( i=0; i<db.rows.size; i++ )
		{
			if( isDefined( db.rows[ i ][ index ] ) && _if( db.rows[ i ][ index ], sign, val ) )
			{
				for( j=0; j<db.keys.size; j++ )
				{
					if( isDefined( dict[ db.keys[ j ] ] ) )
						db.rows[ i ][ j ] = dict[ db.keys[ j ] ];
				}
			}
		}
		
		return save( db.dbid );
	}
	
	return false;
}

db_delete( db, key, sign, val )
{
	if( db == -1 )
	{
		return false;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		index = -1;
		for( i=0; i<db.keys.size; i++ )
		{
			if( db.keys[ i ] == key )
			{
				index = i;
				break;
			}
		}
		
		deleted = 0;
		for( i=0; i<db.rows.size; i++ )
		{
			if( isDefined( db.rows[ i ][ index ] ) && _if( db.rows[ i ][ index ], sign, val ) )
			{
				db.rows[ i ] = undefined;
				deleted++;
			}
		}
		
		if( deleted > 0 )
		{
			_r = [];
			for( i=0; i<db.rows.size; i++ )
			{
				if( isDefined( db.rows[ i ] ) )
					_r[ _r.size ] = db.rows[ i ];
			}
			db.rows = _r;
		}
		
		return save( db.dbid );
	}
	
	return false;
}

db_assoc( db, rows )
{
	result = [];
	
	db = level._db[ db ];
	
	for( i=0; i<rows.size; i++ )
	{
		row = [];
		for( j=0; j<db.keys.size; j++ )
		{
			row[ db.keys[ j ] ] = rows[ i ][ j ]; 
		}
		
		result[ result.size ] = row;
	}
	
	return result;
}

db_concat( dbres1, dbres2, key1, key2, db2 )
{
	db2 = level._db[ db2 ];
	
	out = [];
	for(i=0; i<dbres1.size; i++)
	{
		for(j=0; j<dbres2.size; j++)
		{
			out[i] = dbres1[i];
			if(dbres1[i][key1] == dbres2[j][key2])
			{
				for(k=0; k<db2.keys.size; k++)
				{
					out[i][db2.keys[k]] = dbres2[j][db2.keys[k]];
				}
				break;
			}
		}
	}

	return out;
}

_if( a, b, c )
{
	switch( b )
	{
		case "=": return ( a == c );
		case "!=": return ( a != c );
		case "<": return ( a < c );
		case ">": return ( a > c );
		case "<=": return ( a <= c );
		case ">=": return ( a >= c );
		default: return false;
	}
}

debug( msg )
{
	if( isDefined(level.dbugging) && level.dbugging )
	{
		iprintln( "CoDDB: " + msg );
		print("CoDDB: " + msg + "\n" );
	}
	
}

dumpdbs()
{
	for( i=0; i<level._db.size; i++ )
	{
		dumpdb( level._db[ i ] );
	}
}

dumpdb( db )
{
		
	self iprintln( "DataBase:" + db.name );
		
	_keys = " | ";
	for(j=0; j<db.keys.size; j++)
	{
		_keys += db.keys[j] + " | ";
	}
		
	self iprintln(_keys);
	
	for(j=0; j<db.rows.size; j++)
	{
		_row = " | ";
		for(k=0; k<db.rows[ j ].size; k++)
		{
			if( db.keys[k] == "password" || db.keys[k] == "ip")
				_row += "********** |";
			else
				_row += db.rows[ j ][ k ] + " | ";
		}
		self iprintln(_row);
	}
}

query( db, q )
{
	q = codam\utils::splitArray( q,  " ");

	if( q[0] == "select" )
		result = [];
	else
		result = false;

	if( db == -1 )
	{
		return result;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		
		args = [];
		operation = q[ 0 ];
		for( i=1; i<q.size; i++ )
		{
			if( q[ i ] != "" )
			{
				args[ args.size ] = q[ i ];
			}
		}

		return _queue( operation, db, args );
	}

	return result;
}

_queue( operation, db, args )
{
	id = db.queueId;
	db.queueId++;
	
	thread _queuethread( db, operation, id, args );

	while( !isDefined( db.queueResult[ id ] ) )
	{
		wait 0.05;
	}

	return db.queueResult[ id ];
	
}

_queuethread( db, operation, id, args )
{
	index = db.queue.size;
	db.queue[ index ] = [];
	db.queue[ index ][ 0 ] = operation;
	db.queue[ index ][ 1 ] = id;
	db.queue[ index ][ 2 ] = args;

	if( db.queue_active )
	{
		return;
	}

	db.queue_active = true;
	
	i = 0;
	while( true )
	{
		if( i == db.queue.size )
		{
			db.queue_active = false;
			db.queue = [];
			return;
		}

		if( !db.inuse )
		{
			switch( db.queue[ i ][0] )
			{
				_id = db.queue[ i ][ 1 ];
				_args = db.queue[ i ][ 2 ];
				case "insert": 
					result = db_insert( db.dbid, _args[ 0 ]);
					db.queueResult[ _id ] = result;
					break;
				case "update": 
					result = db_update( db.dbid, _args[ 0 ], _args[ 1 ], _args[ 2 ], _args[ 3 ] );
					db.queueResult[ _id ] = result;
					break;
				case "delete":
					result = db_delete( db.dbid, _args[ 0 ], _args[ 1 ], _args[ 2 ], _args[ 3 ] );
					db.queueResult[ _id ] = result;	
					break;
				case "select":
					result = db_select( db.dbid, _args[ 0 ], _args[ 1 ], _args[ 2 ] );
					db.queueResult[ _id ] = result;	
					break;		
			}
			return;
		}
		i++;
	}
}
