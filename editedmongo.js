"use strict";

//underscore is a toolkit for collection/function/object manipulation
var _     = require('underscore')
//db-nuodb database bindings
var nuo = require('db-nuodb')

//the name of this file/package
var name = "nuo-store"


/*
native$ = object => use object as query, no meta settings
native$ = array => use first elem as query, second elem as meta settings
*/

/*
&& - guard operator
If the first operand is truthy
	then return the second
	else return the first
	
|| - logical operator
If first operand is truthy
	then return the first
	else return the second
	
? : - terenary operator
condition ? valueIfTrue : valueIfFalse

So below means:
if (obj && obj.toHexString)
	obj.toHexString()
else
	''+obj
	
*/
function idstr( obj ) {
  return ( obj && obj.toHexString ) ? obj.toHexString() : ''+obj
}


//BSON is a part of the mongo-db driver - may not work for me.
function makeid(hexstr) {
  if( _.isString(hexstr) && 24 == hexstr.length ) {
    try {
      if( nuo.BSONNative ) {
        return new nuo.BSONNative.ObjectID(hexstr)
      }
      else {
        return new nuo.BSONPure.ObjectID(hexstr)
      }
    }
    catch(e) {
      return hexstr;
    }
  }
  else return hexstr;
}


function fixquery(qent,q) {
  var qq = {};

  if( !q.native$ ) {
    for( var qp in q ) {
      if( !qp.match(/\$$/) ) {
        qq[qp] = q[qp]
      }
    }
    if( qq.id ) {
      qq._id = makeid(qq.id)
      delete qq.id
    }
  }
  else {
    qq = _.isArray(q.native$) ? q.native$[0] : q.native$
  }

  return qq
}


function metaquery(qent,q) {
  var mq = {}

  if( !q.native$ ) {

    if( q.sort$ ) {
      for( var sf in q.sort$ ) break;
      var sd = q.sort$[sf] < 0 ? 'descending' : 'ascending'
      mq.sort = [[sf,sd]]
    }

    if( q.limit$ ) {
      mq.limit = q.limit$
    }

    if( q.skip$ ) {
      mq.skip = q.skip$
    }

    if( q.fields$ ) {
      mq.fields = q.fields$
    }
  }
  else {
    mq = _.isArray(q.native$) ? q.native$[1] : mq
  }

  return mq
}




module.exports = function(opts) {
  var seneca = this
  var desc

  var dbinst  = null
  var collmap = {}
  var specifications = null


  function error(args,err,cb) {
    if( err ) {
      seneca.log.error('entity',err,{store:name})
      return true;
    }
    else return false;
  }



  function configure(spec,cb) {
    specifications = spec

    // defer connection
    // TODO: expose connection action
    if( !_.isUndefined(spec.connect) && !spec.connect ) {
      return cb()
    }


	// if spec is of type string, set conf to null, otherwise set conf = spec
    var conf = 'string' == typeof(spec) ? null : spec
//having checked to make sure the specification is not null (above), connect to the database
    //if conf is null, use a regular expression to break up the string stored in spec, then allocate the correct parts of spec to the
    //correct parts of conf.
    if( !conf ) {
      conf = {}
      var urlM = /^nuo:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec);
      conf.name   = urlM[7]
      conf.port   = urlM[6]
      conf.server = urlM[4]
      conf.username = urlM[2]
      conf.password = urlM[3]

	//if the port is defined, parse it into an integer value, else set the value to null
      conf.port = conf.port ? parseInt(conf.port,10) : null
    }

	// if conf.host is truthy, return that, otherwise return conf.server.
    conf.host = conf.host || conf.server
    conf.username = conf.username || conf.user
    conf.password = conf.password || conf.pass

 //seneca.util.deepextend is used to override default options with those passed from the user, 
 //maintains default options not being overwritten
    var dbopts = seneca.util.deepextend({
      native_parser:false,
      auto_reconnect:true,
      w:1
    },conf.options)

//if the replicaset value is not null, 
//then setup a db server for each server defined in the replicaset
// 	and use this server set in the creation of your database connection object
//else create a new db connection object without and servers defined

    if( conf.replicaset ) {
      var rservs = []
      for( var i = 0; i < conf.replicaset.servers.length; i++ ) {
	var servconf = conf.replicaset.servers[i]
	rservs.push(new nuo.Server(servconf.host,servconf.port,dbopts))
      }
      var rset = new nuo.ReplSetServers(rservs)
      dbinst = new nuo.Db( conf.name, rset, dbopts )
    }
    else {
      dbinst = new nuo.Database({
        hostname: conf.host,
        user: conf.user,
        password: conf.password,
        database: conf.name,
      }).on('error', function(error) {
        console.log('ERROR: ' + error);
    }).on('ready', function(server) {
        console.log('Connected to ' + server.hostname + ' (' + server.version + ')');
    })
    }
  //connect to the database object created
    dbinst.connect(function(err){
      if( err ) {
        return seneca.die('open',err,conf);
      }
    })
  } // end of configure method

//function getcoll gets the collection from the database and returns it
  function getcoll(args,ent,cb) {
    var canon = ent.canon$({object:true})

//if canon.base is true 
//then collname=canon.base+' '+canon.name
//else collname=' '+canon.name
    var collname = (canon.base?canon.base+'_':'')+canon.name

//if the collmap object does not contain the collname vrble (if the collection is not found in our set of colletions...)
// use the db connection to check again if the collection is present in the db and return if it is
//else return the collection as you have it
    if( !collmap[collname] ) {
      dbinst.collection(collname, function(err,coll){
        if( !error(args,err,cb) ) {
          collmap[collname] = coll
          cb(null,coll);
        }
      })
    }
    else {
      cb(null,collmap[collname])
    }
  } //end of getcoll method





  var store = {
    name:name,

    //if db connection exists, close it
    close: function(args,cb) {
      if(dbinst) {
        dbinst.close(cb)
      }
      else return cb();
    },


    save: function(args,cb) {
      var ent = args.ent
// !! returns the true/false boolean value of the argument, so update will equal true or false.
//if ent has an ID
      var update = !!ent.id;

      getcoll(args,ent,function(err,coll){
        if( !error(args,err,cb) ) { //if the getcoll function was not an error.....
          var entp = {};

          var fields = ent.fields$()
          fields.forEach( function(field) {
            entp[field] = ent[field]
          })
			//
          if( !update && void 0 != ent.id$ ) {
            entp._id = makeid(ent.id$)
          }

			// if there is an id present for the data received (data being stored) then update the row
          if( update ) {
            var q = {_id:makeid(ent.id)}
            delete entp.id
			// use db api driver to update the db
            coll.update(q,{$set: entp}, {upsert:true},function(err,update){
              if( !error(args,err,cb) ) {
                seneca.log.debug('save/update',ent,desc)
                cb(null,ent)
              }
            })
          }
          //otherwise (if there is NOT an id present for the data being received) then insert the new row
          else {
          //use db api driver to insert a new row
            coll.insert(entp,function(err,inserts){
              if( !error(args,err,cb) ) {
                ent.id = idstr( inserts[0]._id )

                seneca.log.debug('save/insert',ent,desc)
                cb(null,ent)
              }
            })
          }
        }
      }) //end of getcoll method call
    }, //end of save function


    load: function(args,cb) {
      var qent = args.qent
      var q    = args.q

      getcoll(args,qent,function(err,coll){
        if( !error(args,err,cb) ) {
          var mq = metaquery(qent,q)
          var qq = fixquery(qent,q)

          coll.findOne(qq,mq,function(err,entp){
            if( !error(args,err,cb) ) {
              var fent = null;
              if( entp ) {
                entp.id = idstr( entp._id )
                delete entp._id;

                fent = qent.make$(entp);
              }

              seneca.log.debug('load',q,fent,desc)
              cb(null,fent);
            }
          });
        }
      })
    }, // end of the load function


    list: function(args,cb) {
      var qent = args.qent
      var q    = args.q

      getcoll(args,qent,function(err,coll){
        if( !error(args,err,cb) ) {
          var mq = metaquery(qent,q)
          var qq = fixquery(qent,q)

          coll.find(qq,mq,function(err,cur){
            if( !error(args,err,cb) ) {
              var list = []

              cur.each(function(err,entp){
                if( !error(args,err,cb) ) {
                  if( entp ) {
                    var fent = null;
                    if( entp ) {
                      entp.id = idstr( entp._id )
                      delete entp._id;

                      fent = qent.make$(entp);
                    }
                    list.push(fent)
                  }
                  else {
                    seneca.log.debug('list',q,list.length,list[0],desc)
                    cb(null,list)
                  }
                }
              })
            }
          })
        }
      })
    }, //end of the list function


    remove: function(args,cb) {
      var qent = args.qent
      var q    = args.q

      var all  = q.all$ // default false
      var load  = _.isUndefined(q.load$) ? true : q.load$ // default true

      getcoll(args,qent,function(err,coll){
        if( !error(args,err,cb) ) {
          var qq = fixquery(qent,q)

          if( all ) {
            coll.remove(qq,function(err){
              seneca.log.debug('remove/all',q,desc)
              cb(err)
            })
          }
          else {
            var mq = metaquery(qent,q)
            coll.findOne(qq,mq,function(err,entp){
              if( !error(args,err,cb) ) {
                if( entp ) {
                  coll.remove({_id:entp._id},function(err){
                    seneca.log.debug('remove/one',q,entp,desc)

                    var ent = load ? entp : null
                    cb(err,ent)
                  })
                }
                else cb(null)
              }
            })
          }
        }
      })
    }, // end of the remove function

    native: function(args,done) {
      dbinst.collection('seneca', function(err,coll){
        if( !error(args,err,done) ) {
          coll.findOne({},{},function(err,entp){
            if( !error(args,err,done) ) {
              done(null,dbinst)
            }
            else {
              done(err)
            }
          })
        }
        else {
          done(err)
        }
      })
    } // end of the native function
  } //end of store vrble definition


  var meta = seneca.store.init(seneca,opts,store)
  desc = meta.desc


  seneca.add({init:store.name,tag:meta.tag},function(args,done){
    configure(opts,function(err){
      if( err ) return seneca.die('store',err,{store:store.name,desc:desc});
      return done();
    })
  })


  return {name:store.name,tag:meta.tag}
} // end of module.exports definition












