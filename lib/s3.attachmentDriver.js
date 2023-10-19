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

"use strict" ;



const S3k = require( 's3k-core' ) ;

const Promise = require( 'seventh' ) ;

const stream = require( 'stream' ) ;
const path = require( 'path' ) ;
//const url = require( 'url' ) ;

const hash = require( 'hash-kit' ) ;
const events = require( 'events' ) ;

const ErrorStatus = require( 'error-status' ) ;

const log = require( 'logfella' ).global.use( 'roots-db:s3' ) ;



const storageEndpointFoolproof = new Map() ;



function S3AttachmentDriver( collection ) {
	this.collection = collection ;
	this.appendExtension = collection.attachmentAppendExtension ;

	var bucket , prefix ,
		parts = collection.attachmentConfig.pathname.split( '/' ).filter( part => part.length ) ;

	if ( parts.length < 2 ) {
		throw new Error( "S3AttachmentDriver: missing bucket and prefix part in S3 driver URL" ) ;
	}

	bucket = parts.shift() ;
	prefix = parts.join( '/' ) ;

	if ( prefix[ prefix.length - 1 ] !== '/' ) { prefix += '/' ; }

	var s3Config = {
		endpoint: collection.attachmentConfig.hostname ,
		accessKeyId: collection.attachmentConfig.username ,
		secretAccessKey: collection.attachmentConfig.password ,
		bucket ,
		prefix
	} ;

	let fullEndpoint = s3Config.endpoint + '/' + s3Config.bucket + '/' + prefix ;

	if ( storageEndpointFoolproof.has( fullEndpoint ) ) {
		let firstCollectionUrl = storageEndpointFoolproof.get( fullEndpoint ) ;

		if ( firstCollectionUrl !== collection.url ) {
			log.error(
				"Multiple collections should not share the same S3 storage endpoint: %s\nFirst collection using it: %s\nNew collection trying to use it: %s" ,
				fullEndpoint ,
				firstCollectionUrl ,
				collection.url
			) ;
			let error = new Error( "Multiple collections should not share the same S3 storage endpoint: " + fullEndpoint ) ;
			error.code = 'storageEndpointSharing' ;
			throw error ;
		}
	}
	else {
		storageEndpointFoolproof.set( fullEndpoint , collection.url ) ;
	}

	this.s3 = new S3k( s3Config ) ;
}

module.exports = S3AttachmentDriver ;
S3AttachmentDriver.prototype = Object.create( events.prototype ) ;
S3AttachmentDriver.prototype.constructor = S3AttachmentDriver ;

S3AttachmentDriver.prototype.type = 's3' ;



S3AttachmentDriver.prototype.initAttachment = function( attachment ) {
	// First, check if the ID is set
	if ( ! attachment.id ) {
		attachment.id = hash.randomBase36String( 24 ) ;
		if ( this.appendExtension && attachment.extension ) { attachment.id += '.' + attachment.extension ; }
	}

	attachment.path = path.join( attachment.documentId , attachment.id ) ;

	// It always ends with a slash, so we can concat it immediately
	attachment.publicUrl = this.collection.attachmentPublicBaseUrl ?
		this.collection.attachmentPublicBaseUrl + attachment.path :
		null ;

	//console.log( ".initAttachment():" , attachment.path ) ;
} ;



S3AttachmentDriver.prototype.load = async function( attachment ) {
	var data = await this.s3.getObject( { Key: attachment.path } ) ;
	//console.log( "load data:" , data ) ;
	return data.Body ;
	//return data.Body.toString() ;
} ;



S3AttachmentDriver.prototype.getReadStream = function( attachment ) {
	return this.s3.getObjectStream( { Key: attachment.path } ) ;
} ;



S3AttachmentDriver.prototype.save = async function( attachment ) {
	if ( typeof attachment.incoming === 'string' || Buffer.isBuffer( attachment.incoming ) ) {
		return this.saveContent( attachment ) ;
	}
	else if ( attachment.incoming instanceof stream.Readable ) {
		return this.saveStream( attachment ) ;
	}

	log.error( "S3AttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
	throw new Error( "[roots-db] S3AttachmentDriver, type of data is not supported, should be string, Buffer or ReadableStream" ) ;
} ;



S3AttachmentDriver.prototype.saveContent = async function( attachment ) {
	var result = await this.s3.putObject( { Key: attachment.path , Body: attachment.incoming } ) ;
	//console.log( "saveContent result:" , result ) ;
} ;



S3AttachmentDriver.prototype.saveStream = async function( attachment ) {
	var result = await this.s3.upload( { Key: attachment.path , Body: attachment.incoming } ) ;
	//console.log( "saveStream result:" , result ) ;
} ;



S3AttachmentDriver.prototype.delete = async function( attachment ) {
	var result = await this.s3.deleteObject( { Key: attachment.path } ) ;
	//console.log( "delete result:" , result ) ;
} ;



// Delete all attachment for the current document
S3AttachmentDriver.prototype.deleteAllInDocument = async function( documentId ) {
	var dirPath = documentId + '/' ;
	log.debug( "S3AttachmentDriver#deleteAllInDocument() deleting '%s'" , dirPath ) ;
	if ( ! dirPath ) { return Promise.resolved ; }
	var result = await this.s3.deleteDirectory( { Directory: dirPath } ) ;
	//console.log( "delete result:" , result ) ;
} ;



// Delete all attachment for the current collection
S3AttachmentDriver.prototype.clear = async function() {
	var result = await this.s3.deleteDirectory( { Directory: '' } ) ;
	//console.log( "delete result:" , result ) ;
} ;

