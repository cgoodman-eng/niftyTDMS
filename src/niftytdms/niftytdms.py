from datetime import datetime, timedelta, timezone
from enum import Enum
import struct


# =================================================================================================
# TDMS Data Types
# =================================================================================================

class TdmsDataType(Enum):
  tdsTypeVoid = 0x00000000
  tdsTypeI8   = 0x00000001
  tdsTypeI16  = 0x00000002
  tdsTypeI32  = 0x00000003
  tdsTypeI64  = 0x00000004
  tdsTypeU8   = 0x00000005
  tdsTypeU16  = 0x00000006
  tdsTypeU32  = 0x00000007
  tdsTypeU64  = 0x00000008
  tdsTypeSingleFloat    = 0x00000009
  tdsTypeDoubleFloat    = 0x0000000A
  tdsTypeExtendedFloat  = 0x0000000B
  tdsTypeSingleFloatWithUnit    = 0x00000019
  tdsTypeDoubleFloatWithUnit    = 0x0000001A
  tdsTypeExtendedFloatWithUnit  = 0x0000001B
  tdsTypeString     = 0x00000020
  tdsTypeBoolean    = 0x00000021
  tdsTypeTimeStamp  = 0x00000044
  tdsTypeFixedPoint = 0x0000004F
  tdsTypeComplexSingleFloat = 0x0008000C
  tdsTypeComplexDoubleFloat = 0x0010000D
  tdsTypeDAQmxRawData       = 0xFFFFFFFF



# =================================================================================================
# TDMS Raw Data Structures
# =================================================================================================

class TdmsMaskSettings:
  def __init__(self):
    self.meta_in_seg        = False
    self.raw_data_in_seg    = False
    self.Daqmx_data_in_seg  = False
    self.data_interleaved   = False
    self.data_is_big_endian = False
    self.endianness         = 'little'
    self.new_obj_in_seg     = False

  def __str__(self):
    str_desc = ("Meta in Segment: " + str(self.meta_in_seg) + "\n" +
                "Raw Data in Segment: " + str(self.raw_data_in_seg) + "\n" +
                "Daqmx Data in Segment: " + str(self.Daqmx_data_in_seg) + "\n" +
                "Data Interleaved: " + str(self.data_interleaved) + "\n" +
                "Data is Big Endian: " + str(self.data_is_big_endian) + "\n" +
                "New Object in Segment: " + str(self.new_obj_in_seg))
    return str_desc


class TdmsObject:
  def __init__(self):
    self.path           = ""
    self.clean_path     = ""
    self.raw_data_ind   = 0
    self.num_props      = 0
    self.props          = {}
    self.raw_data_type  = TdmsDataType.tdsTypeVoid
    self.raw_data_dim   = 0
    self.raw_data_num   = 0
    self.raw_data_size  = 0


  def __str__(self):
    str_desc = ("Object Name: " + self.obj_name + "\n" +
                "Object Data: " + str(self.data) + "\n" +
                "Object Properties: " + str(self.prop))
    return str_desc


class TdmsObjTemplate:
  def __init__(self, obj_path, clean_path, raw_data_ind, raw_data_type, raw_data_dim, raw_data_num, raw_data_size):
    self.obj_path = obj_path
    self.clean_path = clean_path
    self.raw_data_ind = raw_data_ind
    self.raw_data_type = raw_data_type
    self.raw_data_dim = raw_data_dim
    self.raw_data_num = raw_data_num
    self.raw_data_size = raw_data_size


class TdmsSegment:
  def __init__(self):
    self.version    = 0
    self.start      = 0
    self.end        = 0
    self.len        = 0
    self.raw_start  = 0
    self.seg_path   = ""
    self.num_objs   = 0
    self.objs       = {}
    self.settings   = None
    self.data       = {}

  def __str__(self):
    str_desc = ("Segment Name: " + self.segment_name + "\n" +
                "Segment Data: " + str(self.segment_data) + "\n" +
                "Segment Meta: " + str(self.segment_meta))
    return str_desc



# =================================================================================================
# TDMS File Data Structures
# =================================================================================================

class TdmsRoot:
  def __init__(self):
    self.groups         = {}
    self.props          = {}
    self.data           = []


class TdmsGroup:
  def __init__(self):
    self.name           = ""
    self.data           = []
    self.props          = {}
    self.channels       = []
    self.channel_data   = {}
    self.channel_props  = {}



# =================================================================================================
# TDMS File Data Structure Utility Functions
# =================================================================================================

class TdmsFileUtil:
  
  @staticmethod
  def TdmsRootAddProps(root, props):
    root.props.update(props)


  @staticmethod
  def TdmsRootAddData(root, data):
    root.data += data


  @staticmethod
  def TdmsRootFetchGroup(root, group_name):
    if group_name not in root.groups:
      root.groups[group_name] = TdmsGroup()
      root.groups[group_name].name = group_name
    return root.groups[group_name]
  

  @staticmethod
  def TdmsGroupAddProps(group, props):
    group.props.update(props)


  @staticmethod
  def TdmsGroupAddData(group, data):
    group.data += data
  

  @staticmethod
  def TdmsGroupAddChannel(group, channel_name):
    if channel_name not in group.channels:
      group.channels.append(channel_name)
      group.channel_data[channel_name] = []
      group.channel_props[channel_name] = {}

  
  @staticmethod
  def TdmsGroupAddChannelData(group, channel_name, channel_data):
    if channel_name not in group.channels:
      TdmsFileUtil.TdmsGroupAddChannel(group, channel_name)

    group.channel_data[channel_name] += channel_data
  
  @staticmethod
  def TdmsGroupAddChannelProps(group, channel_name, channel_props):
    if channel_name not in group.channels:
      TdmsFileUtil.TdmsGroupAddChannel(group, channel_name)

    group.channel_props[channel_name].update(channel_props)



# =================================================================================================
# TDMS File Loader Class
# =================================================================================================

class TdmsLoader:
  def __init__(self):
    self.obj_templates = {}
    self.root = None


  def CreateObjTemplate(self, obj_path, raw_data_ind, raw_data_type, raw_data_dim, raw_data_num, raw_data_size):
    clean_path = TdmsCreateCleanPath(obj_path)
    obj_template = TdmsObjTemplate(obj_path, clean_path, raw_data_ind, raw_data_type, raw_data_dim, raw_data_num, raw_data_size)
    self.obj_templates[clean_path] = obj_template
    return


  def FetchObjTemplate(self, obj_path):
    clean_path = TdmsCreateCleanPath(obj_path)
    if clean_path not in self.obj_templates:
      return None
    return self.obj_templates[clean_path]


  def LoadObject(self, tdms_bytestream, start_ind, endianness):
    obj = TdmsObject()
    obj_path_len, scan_ind      = TdmsExtractU32(tdms_bytestream, start_ind=start_ind, endianness=endianness)
    obj.path, scan_ind          = TdmsExtractString(tdms_bytestream, start_ind=scan_ind, str_len=obj_path_len)
    obj.clean_path              = TdmsCreateCleanPath(obj.path)

    obj.raw_data_ind, scan_ind  = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
    no_raw_data = 0xFFFFFFFF

    if obj.raw_data_ind != no_raw_data and obj.raw_data_ind != 0:
      obj.raw_data_type, scan_ind = TdmsExtractDataType(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
      obj.raw_data_dim, scan_ind  = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
      obj.raw_data_num, scan_ind  = TdmsExtractU64(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
      if TdsmDataLenIsVariable(obj.raw_data_type):
        obj.raw_data_size, scan_ind = TdmsExtractU64(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
      
      if self.FetchObjTemplate(obj.path) == None:
        self.CreateObjTemplate(obj.path, obj.raw_data_ind, obj.raw_data_type, obj.raw_data_dim, obj.raw_data_num, obj.raw_data_size)

    elif obj.raw_data_ind == 0:
      obj_template      = self.FetchObjTemplate(obj.path)
      obj.raw_data_ind  = obj_template.raw_data_ind
      obj.raw_data_type = obj_template.raw_data_type
      obj.raw_data_dim  = obj_template.raw_data_dim
      obj.raw_data_num  = obj_template.raw_data_num
      obj.raw_data_size = obj_template.raw_data_size

    obj.num_props, scan_ind = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
    for j in range(obj.num_props):
      prop_name_len, scan_ind   = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
      prop_name, scan_ind       = TdmsExtractString(tdms_bytestream, start_ind=scan_ind, str_len=prop_name_len)
      prop_datatype, scan_ind   = TdmsExtractDataType(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
      prop_value, scan_ind      = TdmsExtractAuto(prop_datatype, tdms_bytestream, start_ind=scan_ind, endianness=endianness)
      obj.props[prop_name] = prop_value
    
    return obj, scan_ind
  

  def LoadObjectRawData(self, tdms_bytestream, start_ind, obj, endianness):
    obj_data = []
    scan_ind = start_ind

    if obj.raw_data_ind == 0xFFFFFFFF:
      return obj_data, scan_ind

    if obj.raw_data_type == TdmsDataType.tdsTypeString:
      str_lens = []
      for _ in range(obj.raw_data_num):
        str_len, scan_ind = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=endianness)
        str_lens.append(str_len)

      for str_len in str_lens:
        data, scan_ind = TdmsExtractString(tdms_bytestream, start_ind=scan_ind, str_len=str_len)
        obj_data.append(data)
    else:
      for _ in range(obj.raw_data_num):
        data, scan_ind = TdmsExtractAuto(obj.raw_data_type, tdms_bytestream, start_ind=scan_ind, endianness=endianness)
        obj_data.append(data)

    return obj_data, scan_ind


  def LoadSegment(self, tdms_bytestream, start_ind):
    seg = TdmsSegment()
    seg.start = start_ind
    tdms_status, seg.settings, seg.version, seg.len, seg.raw_start = TdmsValidateSegment(tdms_bytestream[start_ind:])
    seg.raw_start += seg.start

    if not tdms_status:
      raise ValueError("TDMS segment not found at given index")
    seg.end = seg.start + seg.len

    if seg.settings.meta_in_seg:
      scan_ind = seg.start + 28   # header is 28 bytes long
      seg.num_objs, scan_ind = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)

      for i in range(seg.num_objs):
        obj, scan_ind = self.LoadObject(tdms_bytestream, scan_ind, endianness=seg.settings.endianness)
        seg.objs[obj.clean_path] = obj

      scan_ind = seg.raw_start
      for obj_clean_path in seg.objs:
        obj = seg.objs[obj_clean_path]
        obj_data, scan_ind = self.LoadObjectRawData(tdms_bytestream, start_ind=scan_ind, obj=obj, endianness=seg.settings.endianness)
        
        seg.data[obj_clean_path] = obj_data

      return seg


  def SegmentToTdms(self, seg):
    for obj_clean_path in seg.objs:
      obj = seg.objs[obj_clean_path]
      if len(obj.clean_path) == 0:  # is root
        if self.root:
          raise ValueError("Root object already created")
        
        else:
          self.root = TdmsRoot()
          TdmsFileUtil.TdmsRootAddProps(self.root, obj.props)
          TdmsFileUtil.TdmsRootAddData(self.root, seg.data[obj.clean_path])
      
      elif len(obj.clean_path.split('-')) == 1:  # is group
        group_name = obj.clean_path
        group = TdmsFileUtil.TdmsRootFetchGroup(self.root, group_name)
        TdmsFileUtil.TdmsGroupAddProps(group, obj.props)
        TdmsFileUtil.TdmsGroupAddData(group, seg.data[obj.clean_path])
      
      else:  # is channel
        group_name, channel_name = obj.clean_path.split('-')
        group = TdmsFileUtil.TdmsRootFetchGroup(self.root, group_name)
        TdmsFileUtil.TdmsGroupAddChannelData(group, channel_name, seg.data[obj.clean_path])
        TdmsFileUtil.TdmsGroupAddChannelProps(group, channel_name, obj.props)
    return

  def LoadFile(self, filepath):
    with open(filepath, 'rb') as file:
      file_content = file.read()
    
    scan_ind = 0
    while scan_ind < len(file_content):
      seg = self.LoadSegment(file_content, scan_ind)
      scan_ind = seg.end

      self.SegmentToTdms(seg)
    return



# =================================================================================================
# TDMS Class/Enum Management Functions
# =================================================================================================

def TdsmDataLenIsVariable(tdms_datatype):
  if tdms_datatype == TdmsDataType.tdsTypeString:
    return True
  else:
    return False



# =================================================================================================
# TDMS Data Extraction Functions
# =================================================================================================

def TdmsExtractBytes(tdms_bytestream, start_ind, no_bytes):
  return tdms_bytestream[start_ind:start_ind+no_bytes], start_ind+no_bytes


def TdmsExtractI8(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+1], byteorder=endianness, signed=True), start_ind+1


def TdmsExtractI16(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+2], byteorder=endianness, signed=True), start_ind+2


def TdmsExtractI32(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+4], byteorder=endianness, signed=True), start_ind+4


def TdmsExtractI64(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+8], byteorder=endianness, signed=True), start_ind+8


def TdmsExtractU8(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+1], byteorder=endianness, signed=False), start_ind+1


def TdmsExtractU16(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+2], byteorder=endianness, signed=False), start_ind+2


def TdmsExtractU32(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+4], byteorder=endianness, signed=False), start_ind+4


def TdmsExtractU64(tdms_bytestream, start_ind, endianness):
  return int.from_bytes(tdms_bytestream[start_ind:start_ind+8], byteorder=endianness, signed=False), start_ind+8


def TdmsExtractSingleFloat(tdms_bytestream, start_ind, endianness):
  if endianness == 'big':
    format_str = '>f'
  else:
    format_str = '<f'

  return struct.unpack(format_str, tdms_bytestream[start_ind:start_ind+4])[0], start_ind+4


def TdmsExtractDoubleFloat(tdms_bytestream, start_ind, endianness):
  if endianness == 'big':
    format_str = '>d'
  else:
    format_str = '<d'

  return struct.unpack(format_str, tdms_bytestream[start_ind:start_ind+8])[0], start_ind+8


def TdmsExtractTimeEpoch(tdms_bytestream, start_ind, endianness):
  scan_ind = 0
  epoch_fraction, scan_ind  = TdmsExtractU64(tdms_bytestream, scan_ind, endianness)
  epoch_seconds, scan_ind   = TdmsExtractI64(tdms_bytestream, scan_ind, endianness)
  return epoch_seconds + (epoch_fraction / 2**64), scan_ind


def TdmsExtractTimeDatetime(tdms_bytestream, start_ind, endianness):
  custom_epoch    = datetime(1904, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
  epoch, scan_ind = TdmsExtractTimeEpoch(tdms_bytestream, start_ind, endianness)
  result_datetime = custom_epoch + timedelta(seconds=epoch)
  return result_datetime, scan_ind


def TdmsExtractTimeString(tdms_bytestream, start_ind, endianness):
  result_datetime, scan_ind = TdmsExtractTimeDatetime(tdms_bytestream, start_ind, endianness)
  return result_datetime.strftime('%Y-%m-%d %H:%M:%S.%f'), scan_ind


def TdmsExtractString(tdms_bytestream, start_ind, str_len):
  return tdms_bytestream[start_ind:start_ind+str_len].decode('utf-8'), start_ind+str_len


def TdmsExtractBool(tdms_bytestream, start_ind, endianness):
  return bool(int.from_bytes(tdms_bytestream[start_ind:start_ind+1], byteorder=endianness, signed=False)), start_ind+1


def TdmsExtractDataType(tdms_bytestream, start_ind, endianness):
  return TdmsDataType(int.from_bytes(tdms_bytestream[start_ind:start_ind+4], byteorder=endianness, signed=False)), start_ind+4


def TdmsExtractAuto(tdms_datatype, tdms_bytestream, start_ind, endianness):
  if tdms_datatype == TdmsDataType.tdsTypeI8:
    return TdmsExtractI8(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeI16:
    return TdmsExtractI16(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeI32:
    return TdmsExtractI32(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeI64:
    return TdmsExtractI64(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeU8:
    return TdmsExtractU8(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeU16:
    return TdmsExtractU16(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeU32:
    return TdmsExtractU32(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeU64:
    return TdmsExtractU64(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeSingleFloat:
    return TdmsExtractSingleFloat(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeDoubleFloat:
    return TdmsExtractDoubleFloat(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeString:
    str_len, start_ind = TdmsExtractU32(tdms_bytestream, start_ind, endianness)
    return TdmsExtractString(tdms_bytestream, start_ind, str_len)
  elif tdms_datatype == TdmsDataType.tdsTypeBoolean:
    return TdmsExtractBool(tdms_bytestream, start_ind, endianness)
  elif tdms_datatype == TdmsDataType.tdsTypeTimeStamp:
    return TdmsExtractTimeDatetime(tdms_bytestream, start_ind, endianness)
  else:
    return None


def TdmsExtractMaskSettings(mask):
  mask_meta_in_seg        = (1 << 1)
  mask_raw_data_in_seg    = (1 << 3)
  mask_Daqmx_data_in_seg  = (1 << 7)
  mask_data_interleaved   = (1 << 5)
  mask_data_is_big_endian = (1 << 6)
  mask_new_obj_in_seg     = (1 << 2)

  seg_mask_settings = TdmsMaskSettings()
  if (mask & mask_meta_in_seg):
    seg_mask_settings.meta_in_seg = True
  if (mask & mask_raw_data_in_seg):
    seg_mask_settings.raw_data_in_seg = True
  if (mask & mask_Daqmx_data_in_seg):
    seg_mask_settings.Daqmx_data_in_seg = True
  if (mask & mask_data_interleaved):
    seg_mask_settings.data_interleaved = True
  if (mask & mask_data_is_big_endian):
    seg_mask_settings.data_is_big_endian = True
    seg_mask_settings.endianness = 'big'
  if (mask & mask_new_obj_in_seg):
    seg_mask_settings.new_obj_in_seg = True
  
  return seg_mask_settings



# =================================================================================================
# General Use Functions
# =================================================================================================

def TdmsCreateCleanPath(input_string):
    valid_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    name_start = False
    result = []
    prev_char_non_keyable = False

    for char in input_string:
      if char == '\'':
        name_start = not name_start
        
      elif char == '/':
        if name_start:
          result.append('_')

        else:
          result = [''.join(result).strip('-_')]
          result.append('-')
          prev_char_non_keyable = True

      elif char not in valid_chars:
        if not prev_char_non_keyable:
          result.append('_')
          prev_char_non_keyable = True
  
      else:
        result.append(char)
        prev_char_non_keyable = False

    return ''.join(result).strip('-_')



# =================================================================================================
# TDMS File Loading Functions
# =================================================================================================

def TdmsValidateSegment(tdms_bytestream):
  tdms_status = True
  toc_mask, version, segment_len, raw_start = 0, 0, 0, 0
  toc_mask_settings = None

  valid_tdms_tag  = 'TDSm'.encode('utf-8')
  tdms_tag        = tdms_bytestream[:4]
  if tdms_tag != valid_tdms_tag:
    print("Invalid tdms file: Tag is not valid")
    tdms_status = False

  else:
    toc_mask          = int.from_bytes(tdms_bytestream[4:8], byteorder='little', signed=False)
    toc_mask_settings = TdmsExtractMaskSettings(toc_mask)

    version     = int.from_bytes(tdms_bytestream[8:12], byteorder=toc_mask_settings.endianness, signed=False)
    segment_len = int.from_bytes(tdms_bytestream[12:20], byteorder=toc_mask_settings.endianness, signed=False) + 28 # header is 28 bytes long
    meta_len    = int.from_bytes(tdms_bytestream[20:28], byteorder=toc_mask_settings.endianness, signed=False)
    raw_start   = 28 + meta_len

  return tdms_status, toc_mask_settings, version, segment_len, raw_start


def TdmsLoadFile(filepath):
  loader = TdmsLoader()
  loader.LoadFile(filepath)

  return loader.root


file1 = TdmsLoadFile(r'..\..\tests\Test_Log-20240731_122619.tdms')
file2 = TdmsLoadFile(r'..\..\tests\Test_Log-20240806_150205.tdms')
