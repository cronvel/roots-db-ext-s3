/*
	Roots DB S3 Driver

	Copyright (c) 2021 Cédric Ronvel

	The MIT License (MIT)

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

/* global describe, it, before, after, beforeEach, expect, teaTime */

"use strict" ;



const rootsDb = require( 'roots-db' ) ;

// Force the S3 driver to be this module
rootsDb.attachmentDriver.s3 = require( '..' ) ;

const util = require( 'util' ) ;
const mongodb = require( 'mongodb' ) ;
const fs = require( 'fs' ) ;

const hash = require( 'hash-kit' ) ;
const string = require( 'string-kit' ) ;
const tree = require( 'tree-kit' ) ;
const streamKit = require( 'stream-kit' ) ;

const Promise = require( 'seventh' ) ;

const ErrorStatus = require( 'error-status' ) ;
ErrorStatus.alwaysCapture = true ;

const doormen = require( 'doormen' ) ;

const logfella = require( 'logfella' ) ;

if ( global.teaTime ) {
	logfella.global.setGlobalConfig( { minLevel: teaTime.cliManager.parsedArgs.log } ) ;
}

const log = logfella.global.use( 'unit-test' ) ;



// Create the world...
const world = new rootsDb.World() ;

// Collections...
var users ;

const s3Config = require( './s3-config.local.json' ) ;

const usersDescriptor = {
	url: 'mongodb://localhost:27017/rootsDb-s3/users' ,
	attachmentUrl: s3Config.attachmentUrl ,
	properties: {
		firstName: {
			type: 'string' ,
			maxLength: 30 ,
			default: 'Joe'
		} ,
		lastName: {
			type: 'string' ,
			maxLength: 30 ,
			default: 'Doe'
		} ,
		godfather: {
			type: 'link' ,
			optional: true ,
			collection: 'users'
		} ,
		connection: {
			type: 'strictObject' ,
			optional: true ,
			of: { type: 'link' , collection: 'users' }
		} ,
		job: {
			type: 'link' ,
			optional: true ,
			collection: 'jobs'
		} ,
		memberSid: {
			optional: true ,
			type: 'string' ,
			maxLength: 30 ,
			tags: [ 'id' ]
		} ,
		avatar: {
			type: 'attachment' ,
			optional: true
		} ,
		publicKey: {
			type: 'attachment' ,
			optional: true
		} ,
		file: {
			type: 'attachment' ,
			optional: true
		}
	} ,
	indexes: [
		{ links: { "job": 1 } } ,
		{ links: { "job": 1 } , properties: { memberSid: 1 } , unique: true }
	] ,
	hooks: {
		afterCreateDocument: document => {
			document.memberSid = '' + document.firstName + ' ' + document.lastName ;
		}
	} ,
	refreshTimeout: 50
} ;



/* Utils */



// drop DB collection: drop all collections
// This is surprisingly slow even for empty collection, so we can't use that all over the place like it should...
function dropDBCollections() {
	//console.log( "dropDBCollections" ) ;
	return Promise.all( [
		dropCollection( users )
	] ) ;
}



// clear DB: remove every item, so we can safely test
function clearDB() {
	return Promise.all( [
		clearCollection( users )
	] ) ;
}



// clear DB indexes: remove all indexes
function clearDBIndexes() {
	return Promise.all( [
		clearCollectionIndexes( users )
	] ).then( () => { log.verbose( "All indexes cleared" ) ; } ) ;
}



function dropCollection( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.drop() )
		.catch( error => {
			if ( error.code === 26 ) { return ; }	// NS not found, nothing to drop!
			throw error ;
		} ) ;
}



function clearCollection( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.deleteMany() )
		.catch( error => {
			if ( error.code === 26 ) { return ; }	// NS not found, nothing to clear!
			throw error ;
		} ) ;
}



function clearCollectionIndexes( collection ) {
	return collection.driver.rawInit()
		.then( () => collection.driver.raw.dropIndexes() )
		.catch( error => {
			if ( error.code === 26 ) { return ; }	// NS not found, nothing to clear!
			throw error ;
		} ) ;
}



/* Tests */



// Force creating the collection
before( async () => {
	users = await world.createAndInitCollection( 'users' , usersDescriptor ) ;
	expect( users ).to.be.a( rootsDb.Collection ) ;
} ) ;



describe( "S3 attachment links" , () => {

	beforeEach( clearDB ) ;

	it( "should create, save, and load an attachment" , async function() {
		this.timeout( 6000 ) ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.equal( {
			filename: 'joke.txt' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'text/plain'
		} ) ;

		await attachment.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'joke.txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/plain'
			}
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tags: [ 'content' ] ,
				inputHint: "file"
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'joke.txt' ,
				contentType: 'text/plain' ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				incoming: undefined ,
				driver: users.attachmentDriver ,
				path: dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: user.file.id ,
			filename: 'joke.txt' ,
			contentType: 'text/plain' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			driver: users.attachmentDriver ,
			path: dbUser.getId() + '/' + details.attachment.id
		} ) ;

		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "should alter meta-data of an attachment" , async function() {
		this.timeout( 6000 ) ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		user.setAttachment( 'file' , attachment ) ;

		await attachment.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		dbUser.file.filename = 'lol.txt' ;
		dbUser.file.contentType = 'text/joke' ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'lol.txt' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'text/joke'
			}
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				type: 'attachment' ,
				tags: [ 'content' ] ,
				inputHint: "file"
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'lol.txt' ,
				contentType: 'text/joke' ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				incoming: undefined ,
				driver: users.attachmentDriver ,
				path: dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'lol.txt' ,
			contentType: 'text/joke' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			driver: users.attachmentDriver ,
			path: dbUser.getId() + '/' + details.attachment.id
		} ) ;

		var content = await dbAttachment.load() ;
		expect( content.toString() ).to.be( "grigrigredin menufretin\n" ) ;
	} ) ;

	it( "should replace an attachment" , async function() {
		this.timeout( 6000 ) ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;

		await attachment.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;

		await expect( dbUser.getAttachment( 'file' ).load()
			.then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;

		var attachment2 = user.createAttachment(
			{ filename: 'hello-world.html' , contentType: 'text/html' } ,
			"<html><head></head><body>Hello world!</body></html>\n"
		) ;

		await dbUser.setAttachment( 'file' , attachment2 ) ;

		// Check that the previous file has been deleted
		expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;

		await attachment2.save() ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'hello-world.html' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'text/html'
			}
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			hostPath: 'file' ,
			schema: {
				optional: true ,
				tags: [ 'content' ] ,
				type: 'attachment' ,
				inputHint: "file"
			} ,
			attachment: {
				id: dbUser.file.id ,
				filename: 'hello-world.html' ,
				contentType: 'text/html' ,
				collectionName: 'users' ,
				documentId: id.toString() ,
				incoming: undefined ,
				driver: users.attachmentDriver ,
				path: dbUser.getId() + '/' + details.attachment.id
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'hello-world.html' ,
			contentType: 'text/html' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			driver: users.attachmentDriver ,
			path: dbUser.getId() + '/' + details.attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( "<html><head></head><body>Hello world!</body></html>\n" ) ;
	} ) ;

	it( "Delete an attachment" , async function() {
		this.timeout( 6000 ) ;
		
		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;

		var attachment = user.createAttachment( { filename: 'joke.txt' , contentType: 'text/plain' } , "grigrigredin menufretin\n" ) ;
		await user.setAttachment( 'file' , attachment ) ;

		await attachment.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;

		await expect( dbUser.getAttachment( 'file' ).load()
			.then( v => v.toString() ) ).to.eventually.be( "grigrigredin menufretin\n" ) ;

		await dbUser.removeAttachment( 'file' ) ;

		// Check that the previous file has been deleted
		expect( () => { fs.accessSync( attachment.path , fs.R_OK ) ; } ).to.throw( Error , { code: 'ENOENT' } ) ;

		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: null
		} ) ;

		var details = dbUser.getAttachmentDetails( 'file' ) ;
		expect( details ).to.be.like( {
			type: 'attachment' ,
			attachment: null
		} ) ;

		expect( () => dbUser.getAttachment( 'file' ) ).to.throw( ErrorStatus , { type: 'notFound' } ) ;
	} ) ;

	it( "should create, save and replace attachments as stream, and load as stream" , async function() {
		this.timeout( 6000 ) ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		var stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
		} ) ;

		var attachment = user.createAttachment( { filename: 'random.bin' , contentType: 'bin/random' } , stream ) ;
		user.setAttachment( 'file' , attachment ) ;
		//log.error( user.file ) ;

		expect( user.file ).to.equal( {
			filename: 'random.bin' ,
			id: user.file.id ,	// Unpredictable
			contentType: 'bin/random'
		} ) ;

		await attachment.save() ;
		await user.save() ;

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				id: user.file.id ,	// Unpredictable
				contentType: 'bin/random'
			}
		} ) ;

		var dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'random.bin' ,
			contentType: 'bin/random' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			driver: users.attachmentDriver ,
			path: dbUser.getId() + '/' + attachment.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		stream = new streamKit.FakeReadable( {
			timeout: 50 , chunkSize: 10 , chunkCount: 3 , filler: 'b'.charCodeAt( 0 )
		} ) ;
		var attachment2 = user.createAttachment( { filename: 'more-random.bin' , contentType: 'bin/random' } , stream ) ;

		await dbUser.setAttachment( 'file' , attachment2 ) ;

		await attachment2.save() ;
		await dbUser.save() ;

		dbUser = await users.get( id ) ;

		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'more-random.bin' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random'
			}
		} ) ;

		dbAttachment = dbUser.getAttachment( 'file' ) ;
		expect( dbAttachment ).to.be.like( {
			id: dbUser.file.id ,
			filename: 'more-random.bin' ,
			contentType: 'bin/random' ,
			collectionName: 'users' ,
			documentId: id.toString() ,
			incoming: undefined ,
			driver: users.attachmentDriver ,
			path: dbUser.getId() + '/' + attachment2.id
		} ) ;

		await expect( dbAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 30 ) ) ;

		// Now load as a stream
		var readStream = await dbAttachment.getReadStream() ;
		var fakeWritable = new streamKit.WritableToBuffer() ;
		readStream.pipe( fakeWritable ) ;
		await Promise.onceEvent( fakeWritable , "finish" ) ;

		expect( fakeWritable.get().toString() ).to.be( 'b'.repeat( 30 ) ) ;
	} ) ;

	it( "should .save() a document with the 'attachmentStreams' option" , async function() {
		this.timeout( 6000 ) ;

		var user = users.createDocument( {
			firstName: 'Jilbert' ,
			lastName: 'Polson'
		} ) ;

		var id = user.getId() ;
		var attachmentStreams = new rootsDb.AttachmentStreams() ;

		attachmentStreams.addStream(
			new streamKit.FakeReadable( {
				timeout: 20 , chunkSize: 10 , chunkCount: 4 , filler: 'a'.charCodeAt( 0 )
			} ) ,
			'file' ,
			{ filename: 'random.bin' , contentType: 'bin/random' }
		) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 4 , filler: 'b'.charCodeAt( 0 )
				} ) ,
				'avatar' ,
				{ filename: 'face.jpg' , contentType: 'image/jpeg' }
			) ;
		} , 100 ) ;

		setTimeout( () => {
			attachmentStreams.addStream(
				new streamKit.FakeReadable( {
					timeout: 20 , chunkSize: 7 , chunkCount: 3 , filler: 'c'.charCodeAt( 0 )
				} ) ,
				'publicKey' ,
				{ filename: 'rsa.pub' , contentType: 'application/x-pem-file' }
			) ;
		} , 200 ) ;

		setTimeout( () => attachmentStreams.end() , 300 ) ;

		await user.save( { attachmentStreams: attachmentStreams } ) ;

		var dbUser = await users.get( id ) ;
		expect( dbUser ).to.equal( {
			_id: id ,
			firstName: 'Jilbert' ,
			lastName: 'Polson' ,
			memberSid: 'Jilbert Polson' ,
			file: {
				filename: 'random.bin' ,
				id: dbUser.file.id ,	// Unpredictable
				contentType: 'bin/random'
			} ,
			avatar: {
				filename: 'face.jpg' ,
				id: dbUser.avatar.id ,	// Unpredictable
				contentType: 'image/jpeg'
			} ,
			publicKey: {
				filename: 'rsa.pub' ,
				id: dbUser.publicKey.id ,	// Unpredictable
				contentType: 'application/x-pem-file'
			}
		} ) ;

		var fileAttachment = dbUser.getAttachment( 'file' ) ;
		expect( fileAttachment ).to.be.partially.like( {
			filename: 'random.bin' ,
			contentType: 'bin/random'
		} ) ;

		await expect( fileAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'a'.repeat( 40 ) ) ;

		var avatarAttachment = dbUser.getAttachment( 'avatar' ) ;

		expect( avatarAttachment ).to.be.partially.like( {
			filename: 'face.jpg' ,
			contentType: 'image/jpeg'
		} ) ;

		await expect( avatarAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'b'.repeat( 28 ) ) ;

		var publicKeyAttachment = dbUser.getAttachment( 'publicKey' ) ;
		expect( publicKeyAttachment ).to.be.partially.like( {
			filename: 'rsa.pub' ,
			contentType: 'application/x-pem-file'
		} ) ;

		await expect( publicKeyAttachment.load().then( v => v.toString() ) ).to.eventually.be( 'c'.repeat( 21 ) ) ;
	} ) ;
} ) ;

