import os
import sys
import bz2
import getopt

stderr = sys.stderr

verbose = 0


def compress_chunk( data, compression_level=9 ):

    # Instantiate a compressor object
    comp = bz2.BZ2Compressor( compression_level )

    # Compress the data
    zdata = comp.compress( data )

    # Finish and close the compressor.
    zdata += comp.flush()

    return zdata


def decompress_1block( zdata, tag ):

    # Instantiate a compressor object
    d = bz2.BZ2Decompressor()

    # Compress the data
    data = d.decompress( zdata )

    # There shouldn't be any more data to decompress since this should be a single block.
    # assert d.needs_input == True, 'Error: needs_input False {}'.format(tag)
    assert len(d.unused_data) == 0, 'Error: len(unused_data) != 0 {}'.format(tag)

    return data


def compress_stream( istream, ostream, compression_level=9 ):

    # How big are the input chunks?
    # The compression level switch is [1-9] which means 100-900k.
    chunk_size = compression_level * 100 * 1024

    n_chunks = 0
    ibytes = 0
    obytes = 0

    while True:

        # Read a chunk of data from the input stream.
        idata = istream.read( chunk_size )

        if not idata:
            break

        # for debugging

        ibytes += len(idata)

        odata = compress_chunk( idata, compression_level )

        obytes += len(odata)

        ostream.write( odata )

        if verbose > 1:
            stderr.write('chunk: {}, idata: {}, odata: {}\n'.format(n_chunks, len(idata), len(odata)) )

        n_chunks += 1


    return n_chunks, ibytes, obytes


def decompress_stream( istream, ostream ):

    buffer_size = 9 * 100 * 1024 + 600 # maximum size of a compressed block.

    nblocks = 0
    ibytes = 0
    obytes = 0

    first_block = True

    header = "BZh91AY&SY"

    inbuf = b''
    assert len(inbuf) == 0

    while True:

        # Read a chunk of data from the input stream.
        idata = istream.read( buffer_size )

        if verbose > 1:
            stderr.write('read {}\n'.format(len(idata)))

        if not idata:
            break

        # for debugging

        ibytes += len(idata)

        is_end = len(idata) < buffer_size

        if first_block:
            first_block = False
            assert len(idata) > len(header)
            _head = idata[:10].decode()
            assert idata[:3] == b'BZh' and idata[4:10] == b'1AY&SY', 'Error: header is not BZ2'
            assert _head[:3] == 'BZh' and _head[4:10] == '1AY&SY', 'Error: header is not BZ2 str'
            compression_level = int( _head[3] )
            _header = 'BZh{:d}1AY&SY'.format(compression_level)
            header = bytes( _header, encoding='ascii' )
            if verbose > 1:
                stderr.write('detected compression level {} {}\n'.format(compression_level, header))


        # Concatenate to the input buffer.
        inbuf += idata

        zblocks = []

        while len(inbuf) > 0:

            # Check that we're at the start of a block.
            assert inbuf.startswith(header), 'Error: not at the start {} {}'.format(inbuf[:10], header)

            pos = inbuf.find( header, len(header) )

            if pos == -1:
                # No block header found.
                if is_end:
                    pos = len(inbuf)
                else:
                    break

            zblocks.append( inbuf[:pos] )
            if verbose > 2:
                stderr.write('zblock {} {}\n'.format(pos, len(inbuf)))

            inbuf = inbuf[pos:]


        for i, zblock in enumerate(zblocks):
            zsize = len(zblock)
            data = decompress_1block( zblock, tag=nblocks+i)
            if verbose > 2:
                stderr.write('decomp block {} {}\n'.format(zsize, len(data)))

            ostream.write(data)

            obytes += len(data)


        nblocks += len(zblocks)


    return nblocks, ibytes, obytes


def usage(os):
    os.write('Usage: pbzip2.py <options> <files>')
    os.write(' -h | --help           print this message\n')
    os.write(' -l | --level=<#>      compression level [1-9] (default: 9)\n')
    os.write(' -c | --stdout         write the compressed data to stdout instead of <input>.bz2\n')
    os.write(' -i | --stdin          read the input data from stdin instead of a file.\n')
    os.write(' -v | --verbose        increment the verbosity level.\n')
    os.write(' -f | --force          force overwrite of exiting .bz2 file if present.\n')
    os.write(' -d | --decompress     decompress the input stream.\n')
    os.write(' -z | --compress       compress the input stream (default).\n')


def main():

    try:
        args, opts = getopt.getopt( sys.argv[1:], 'hl:civfdz', ['help', 'level=', 'stdout', 'stdin', 'verbose', 'force', 'decompress', 'compress'])
    except getopt.GetoptError as e:
        assert False, 'Error in getopt {}'.format(e)

    from_stdin = False
    to_stdout = False
    compression_level = 9
    force_overwrite = False

    global verbose
    verbose = 0

    compress = True

    for opt, arg in args:
        if opt in ('-h', '--help'):
            usage( sys.stdout )
            sys.exit(0)
        elif opt in ('-l', '--level'):
            compression_level = int(arg)
        elif opt in ('-v', '--verbose'):
            verbose += 1
        elif opt in ('-c', '--stdout'):
            to_stdout = True
        elif opt in ('-i', '--stdin'):
            from_stdin = True
        elif opt in ('-f', '--force'):
            force_overwrite = True
        elif opt in ('-z', '--compress'):
            compress = True
        elif opt in ('-d', '--decompress'):
            compress = False
        else:
            assert False, 'Error: option {} not handled.\n'.format(opt)

    in_files = opts

    if from_stdin:
        if not to_stdout and verbose:
            stderr.write('Warning: writing to stdout by default when reading from stdin.\n')

        to_stdout = True

        assert len(in_files) == 0, 'Error: cannot read from stdin and a file list'

    else:
        assert len(in_files) > 0, 'Error: no input files given'

    if verbose:
        stderr.write("Options: compression_level={}, verbose={}, force={}, stdout={}, stdin={}, files={}\n".format( compression_level, verbose, force_overwrite, to_stdout, from_stdin, in_files))

    if from_stdin:
        in_files = [ '/dev/stdin' ]

    max_filename_length = max( [len(f) for f in in_files])


    for ifile in in_files:
        assert os.path.isfile(ifile), 'Error: file {} not found'.format(ifile)

        if verbose:
            f = '  {:<' + str(max_filename_length) + 's}: '
            stderr.write( f.format(ifile) )

        istream = None
        if from_stdin:
            istream = sys.stdin.buffer
        else:
            if not compress:
                assert ifile.endswith('.bz2'), 'Error: input filename does not end in bz2'
            istream = open(ifile, 'rb')

        if to_stdout:
            ostream = sys.stdout.buffer
        else:
            if compress:
                ofile = ifile + '.bz2'
            else:
                ofile = ifile[:-4]

            if os.path.isfile( ofile ):
                assert force_overwrite, 'Error: will not overwrite exiting file {}. To overwrite, specify --force'.format(ofile)

            ostream = open(ofile, 'wb')

        if compress:
            _, ibytes, obytes = compress_stream( istream, ostream, compression_level )
        else:
            _, ibytes, obytes = decompress_stream( istream, ostream )

        if not from_stdin:
            istream.close()

        if not to_stdout:
            ostream.close()

        if verbose:
            if compress:
                ratio = float(ibytes) / obytes
                bits = float(8*obytes) / ibytes
                percent = 100. * float(ibytes - obytes) / ibytes

                f = '{:.3f}:1, {:.3f} bits/byte, {:.2f}% saved, {} in, {} out.\n'
                #"""   openmpi/openmpi-4.1.4.tar: 11.454:1,  0.698 bits/byte, 91.27% saved, 115025920 in, 10042839 out."""

                stderr.write( f.format( ratio, bits, percent, ibytes, obytes ))
            else:
                stderr.write( 'done\n' )


if __name__ == '__main__':
    main()
