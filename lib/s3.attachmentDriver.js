/*
	Roots DB

	Copyright (c) 2014 - 2021 Cédric Ronvel

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

"use strict" ;



const s3k = require( 's3k' ) ;

const Promise = require( 'seventh' ) ;
const stream = require( 'stream' ) ;
const events = require( 'events' ) ;
//const url = require( 'url' ) ;
const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db:s3' ) ;



function S3AttachmentDriver( collection ) {
	this.collection = collection ;
	//this.client = null ;
	this.storageDirPath = collection.attachmentConfig.pathname ;
	//console.log( "this.storageDirPath:" , this.storageDirPath ) ;
}

module.exports = S3AttachmentDriver ;
S3AttachmentDriver.prototype = Object.create( events.prototype ) ;
S3AttachmentDriver.prototype.constructor = S3AttachmentDriver ;

S3AttachmentDriver.prototype.type = 's3' ;



S3AttachmentDriver.prototype.initAttachment = function( attachment ) {
	attachment.path = path.join( this.storageDirPath , attachment.documentId , attachment.id ) ;
	//console.log( ".initAttachment():" , attachment.path ) ;
} ;



S3AttachmentDriver.prototype.save = async function( attachment ) {
	await fsKit.ensurePath( path.dirname( attachment.path ) ) ;

	if ( typeof attachment.incoming === 'string' || Buffer.isBuffer( attachment.incoming ) ) {
		return this.saveContent( attachment ) ;
	}
	else if ( attachment.incoming instanceof stream.Readable ) {
		return this.saveStream( attachment ) ;
	}

	log.error( "S3AttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
	throw new Error( "[roots-db] S3AttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;

} ;



S3AttachmentDriver.prototype.saveContent = function( attachment ) {
	return fs.promises.writeFile( attachment.path , attachment.incoming ) ;
} ;



S3AttachmentDriver.prototype.saveStream = async function( attachment ) {
	var promise = new Promise() ;

	var fileStream = fs.createWriteStream( attachment.path ) ;
	attachment.incoming.pipe( fileStream ) ;

	fileStream.once( 'error' , error => promise.reject( error ) ) ;

	// Should listen the readable or the writable stream for that?
	attachment.incoming.once( 'end' , () => promise.resolve() ) ;
	fileStream.once( 'end' , () => promise.resolve() ) ;

	return promise ;
} ;



S3AttachmentDriver.prototype.load = function( attachment ) {
	return fs.promises.readFile( attachment.path ).catch( error => {
		switch ( error.code ) {
			case 'ENOENT' :
				throw Error.notFound( "File not found" , error ) ;
			default :
				throw error ;
		}
	} ) ;
} ;



S3AttachmentDriver.prototype.getReadStream = function( attachment ) {
	var fileStream ,
		promise = new Promise() ;

	fileStream = fs.createReadStream( attachment.path , { defaultEncoding: 'binary' } ) ;

	fileStream.once( 'error' , ( error ) => {
		log.error( 'S3AttachmentDriver .getReadStream() error event: %E' , error ) ;

		if ( error.code === 'ENOENT' ) {
			promise.reject( ErrorStatus.notFound( {
				message: 'File not found: ' + error.toString() ,
				safeMessage: 'File not found'
			} ) ) ;
		}
		else {
			promise.reject( error ) ;
		}
	} ) ;

	fileStream.once( 'open' , () => {
		promise.resolve( fileStream ) ;
	} ) ;

	return promise ;
} ;



S3AttachmentDriver.prototype.delete = function( attachment ) {
	return fs.promises.unlink( attachment.path ).catch( error => {
		switch ( error.code ) {
			case 'ENOENT' :
				throw Error.notFound( "File not found" , error ) ;
			default :
				throw error ;
		}
	} ) ;
} ;

