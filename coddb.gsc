
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
		
		id = level._db.size;
		
		db.dbid = id;
		
		level._db[ id ] = db;
		
		
	}
	
	debug( "Loading db " + id );
	
	return id;
	
}

reload( db )
{
	if( db == -1 )
	{
		return false;
	}
	
	db = level._db[ db ];
	
	if( isDefined( db ) )
	{
		level._db[ db.dbid ] = _load( db.name );
		return true;
	}
	
	return false;
}

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
	
	if( fexists( filename ) )
	{
		f = fopen( filename, "r+" );
		data = fread( 0, f );
		fclose(f);
		
		if( isDefined( data ) )
		{
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
			db.rows = [];
			
			
			for(i=1; i<data.size; i++)
			{
				iprintlnbold(data[i]);
				row = codam\utils::splitArray(data[i], "%");
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
	
	db = level._db[ db ];
	
	if( isDefined( db ) && fexists( db.filename ) )
	{
		data = db.keys[ 0 ];
		for( i=1; i<db.keys.size; i++ )
		{
			data += "%" + db.keys[ i ];
		}
		
		data += "%" + db.aikey + "%" + db.ai + "\n";
		
		for( i=0; i<db.rows.size; i++ )
		{
			row = db.rows[ i ];
			
			if( !isDefined( row ) )
				continue;
			
			data += row[0];
			for( j=1; j<row.size; j++ )
			{
				data += "%" + row[ j ];
			}
			data += "\n";
		}
		
		f = fopen( db.filename, "w" );
		if( f == -1 )
			return false;
		
		fwrite( f, data );
		fclose( f );
		
		debug( "saved" );
		
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
			else if( _if( db.rows[ i ][ index ], sign, val ) )
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
			if( _if( db.rows[ i ][ index ], sign, val ) )
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
			if( _if( db.rows[ i ][ index ], sign, val ) )
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
		
	iprintln( "DataBase:" + db.name );
		
	_keys = " | ";
	for(j=0; j<db.keys.size; j++)
	{
		_keys += db.keys[j] + " | ";
	}
		
	iprintln(_keys);
	
	for(j=0; j<db.rows.size; j++)
	{
		_row = " | ";
		for(k=0; k<db.rows[ j ].size; k++)
		{
			_row += db.rows[ j ][ k ] + " | ";
		}
		iprintln(_row);
	}
}

/*
* Test ( i dont have cod extended so... )
*/

/* 
fopen(name, m)
{
	if( !isDefined( level._files ) )
	{
		level._files = [];
		
		level._files[0] = [];
		level._files[0]["path"] = level.workingdir + "users.dat";
		level._files[0]["content"] = "id%name%password%id%0\n";
		level._files[0]["mode"] = "closed";
		
		level._files[1] = [];
		level._files[1]["path"] = level.workingdir + "stats.dat";
		level._files[1]["content"] = "user%kills%death%None%0\n";
		level._files[1]["mode"] = "closed";
	}
	
	for(i=0; i<level._files.size; i++)
	{
		if( name == level._files[ i ][ "path" ])
		{
			return i;
		}
	}
	
	return -1;
}

fclose(f)
{
	level._files[f]["mode"] = "closed";
}


fwrite(f, data)
{
	file = level._files[ f ];
	if( isDefined( file ) )
	{
		switch(file["mode"])
		{
			case "w":
			case "w+":
				level._files[f]["content"] = data;
				break;
			case "a":
				level._files[f] = files["content"] + data;
				break;
		}
	}
}

fread(size, f)
{
	if( size == 0 )
	{
		return level._files[f]["content"];
	}
	else
	{
		out = "";
		for(i=0; i<size; i++)
		{
			if( isDefined(level._files[f]["content"][i]) )
			{
				out += level._files[f]["content"][i];
			}
			else
			{
				break;
			}
		}
	}
}

fexists(f)
{
	for(i=0; i<level._files.size; i++)
	{
		if( name == level._files[ i ][ "path" ])
		{
			return true;
		}
	}
	
	return false;
}
*/
