import os
from apache_beam.io import filebasedsink
from apache_beam.io.iobase import Write
from apache_beam.coders import coders
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.transforms.display import DisplayDataItem

class Utf8TextSink(filebasedsink.FileBasedSink):
  """A custom text sink for demoing.
  
  mime_type to display nicely in GCS
  Will add env var DEMO_HEADER as a header to top of files
  or a placeholder if not set.
  """

  def __init__(self,
               file_path_prefix,
               file_name_suffix='',
               append_trailing_newlines=True,
               num_shards=0,
               shard_name_template=None,
               coder=coders.ToBytesCoder(),  # type: coders.Coder
               compression_type=CompressionTypes.AUTO,
               mime_type='text/plain; charset=utf-8'):
    """Initialize a TextSink that by default writes 

    Args:
      file_path_prefix: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards), and
        end in a common extension, if given by file_name_suffix. In most cases,
        only this argument is specified and num_shards, shard_name_template, and
        file_name_suffix use default values.
      file_name_suffix: Suffix for the files written.
      append_trailing_newlines: indicate whether this sink should write an
        additional newline char after writing each element.
      num_shards: The number of files (shards) used for output. If not set, the
        service will decide on the optimal number of shards.
        Constraining the number of shards is likely to reduce
        the performance of a pipeline.  Setting this value is not recommended
        unless you require a specific number of output files.
      shard_name_template: A template string containing placeholders for
        the shard number and shard count. When constructing a filename for a
        particular shard number, the upper-case letters 'S' and 'N' are
        replaced with the 0-padded shard number and shard count respectively.
        This argument can be '' in which case it behaves as if num_shards was
        set to 1 and only one file will be generated. The default pattern used
        is '-SSSSS-of-NNNNN' if None is passed as the shard_name_template.
      coder: Coder used to encode each line.
      compression_type: Used to handle compressed output files. Typical value
        is CompressionTypes.AUTO, in which case the final file path's
        extension (as determined by file_path_prefix, file_name_suffix,
        num_shards and shard_name_template) will be used to detect the
        compression.

    Returns:
      A Utf8TextSink object usable for writing.
    """
    super(Utf8TextSink, self).__init__(
        file_path_prefix,
        file_name_suffix=file_name_suffix,
        num_shards=num_shards,
        shard_name_template=shard_name_template,
        coder=coder,
        mime_type=mime_type,
        compression_type=compression_type)
    self._append_trailing_newlines = append_trailing_newlines
  
  def get_header(self):
    header = os.environ.get('DEMO_HEADER')
    if header == None:
      return "no header was set!\n"
    return '%s\n' % header

  def open(self, temp_path):
    file_handle = super(Utf8TextSink, self).open(temp_path)
    header = self.get_header()
    if header is not None:
      file_handle.write(coders.ToBytesCoder().encode(header))
      if self._append_trailing_newlines:
        file_handle.write(b'\n')
    return file_handle

  def close(self, file_handle):
    super(Utf8TextSink, self).close(file_handle)

  def display_data(self):
    dd_parent = super(Utf8TextSink, self).display_data()
    dd_parent['append_newline'] = DisplayDataItem(
        self._append_trailing_newlines, label='Append Trailing New Lines')
    return dd_parent

  def write_encoded_record(self, file_handle, encoded_value):
    """Writes a single encoded record."""
    file_handle.write(encoded_value)
    if self._append_trailing_newlines:
      file_handle.write(b'\n')
