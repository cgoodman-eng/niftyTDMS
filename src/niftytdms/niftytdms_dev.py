#from nptdms import TdmsFile
#tdms_file = TdmsFile.read("./Test_Log-20240731_122619.tdms")
import pprint
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
# TDMS Data Structures
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


class TDMSObject:
  def __init__(self):
    self.path           = ""
    self.raw_data_ind   = 0
    self.num_props      = 0
    self.props          = {}
    self.raw_data_type  = TdmsDataType.tdsTypeVoid
    self.raw_data_dim   = 0
    self.raw_data_len   = 0
    self.raw_data_size  = 0


  def __str__(self):
    str_desc = ("Object Name: " + self.obj_name + "\n" +
                "Object Data: " + str(self.obj_data) + "\n" +
                "Object Properties: " + str(self.prop))
    return str_desc


class TdmsSegment:
  def __init__(self):
    self.version    = 0
    self.start      = 0
    self.end        = 0
    self.len        = 0
    self.meta_len   = 0
    self.seg_path   = ""
    self.num_objs   = 0
    self.objs       = []
    self.settings   = None

  def __str__(self):
    str_desc = ("Segment Name: " + self.segment_name + "\n" +
                "Segment Data: " + str(self.segment_data) + "\n" +
                "Segment Meta: " + str(self.segment_meta))
    return str_desc


class TdmsFile:
  def __init__(self):
    self.filepath = ""
    self.meta_data = {}
    self.data_frames = {}



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

    return struct.unpack_from(format_str, tdms_bytestream[start_ind:start_ind+4])[0], start_ind+4


def TdmsExtractDoubleFloat(tdms_bytestream, start_ind, endianness):
    if endianness == 'big':
      format_str = '>d'
    else:
      format_str = '<d'

    return struct.unpack_from(format_str, tdms_bytestream[start_ind:start_ind+8])[0], start_ind+8


def TdmsExtractString(tdms_bytestream, start_ind, str_len):
  return tdms_bytestream[start_ind:start_ind+str_len].decode('utf-8'), start_ind+str_len


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
# TDMS File Loading Functions
# =================================================================================================

def ValidateTdmsSegment(tdms_bytestream):
  tdms_status = True
  toc_mask, version, segment_len, meta_len = 0, 0, 0, 0
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
    segment_len = int.from_bytes(tdms_bytestream[12:20], byteorder=toc_mask_settings.endianness, signed=False)
    meta_len    = int.from_bytes(tdms_bytestream[20:28], byteorder=toc_mask_settings.endianness, signed=False)

  return tdms_status, toc_mask_settings, version, segment_len, meta_len


def LoadSegment(tdms_bytestream, start_index):
  seg = TdmsSegment()
  seg.start = start_index
  tdms_status, seg.settings, seg.version, seg.len, seg.meta_len = ValidateTdmsSegment(tdms_bytestream[start_index:])

  if not tdms_status:
    raise ValueError("TDMS segment not found at given index")
  seg.end = seg.start + seg.len + 28

  if seg.settings.meta_in_seg:
    scan_ind = seg.start + 28   # header is 28 bytes long
    seg.num_objs, scan_ind = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
    print(f'Number of objects: {seg.num_objs}')

    for i in range(seg.num_objs):
      print(f'\tObject {i+1}')
      obj = TDMSObject()
      obj_path_len, scan_ind      = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
      obj.path, scan_ind          = TdmsExtractString(tdms_bytestream, start_ind=scan_ind, str_len=obj_path_len)
      print(f'\tObject Path: {obj.path}')
      obj.raw_data_ind, scan_ind  = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
      no_raw_data = 0xFFFFFFFF
      if obj.raw_data_ind != no_raw_data and obj.raw_data_ind != 0:
        obj.raw_data_type, scan_ind = TdmsExtractDataType(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
        obj.raw_data_dim, scan_ind  = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
        obj.raw_data_len, scan_ind  = TdmsExtractU64(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
        if TdsmDataLenIsVariable(obj.raw_data_type):
          obj.raw_data_size, scan_ind = TdmsExtractU64(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)

      print(f'\tRaw Data Index: {obj.raw_data_ind}')
      obj.num_props, scan_ind = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
      print(f'\tNumber of Properties: {obj.num_props}')

      for j in range(obj.num_props):
        print(f'\t\tProperty {j+1}')
        prop_name_len, scan_ind   = TdmsExtractU32(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
        prop_name, scan_ind       = TdmsExtractString(tdms_bytestream, start_ind=scan_ind, str_len=prop_name_len)
        print(f'\t\tProperty Name: {prop_name}')
        prop_datatype, scan_ind   = TdmsExtractDataType(tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
        print(f'\t\tProperty Data Type: {prop_datatype}')
        prop_value, scan_ind      = TdmsExtractAuto(prop_datatype, tdms_bytestream, start_ind=scan_ind, endianness=seg.settings.endianness)
        print(f'\t\tProperty Value: {prop_value}')
        obj.props[prop_name] = prop_value
      
      seg.objs.append(obj)

    return seg


def LoadTdsmFile(filepath):
  segments = []
  with open(filepath, 'rb') as file:
    file_content = file.read()
  
  seg_end = 0
  
  while seg_end < len(file_content):
    seg = LoadSegment(file_content, seg_end)
    segments.append(seg)
    seg_end = seg.end
  
  return segments
  
with open('../../tests/Test_Log-20240731_122619.tdms', 'rb') as file:
  file_content = file.read()

seg_end = 0
segments = []
while seg_end < len(file_content):
  seg = LoadSegment(file_content, seg_end)
  segments.append(seg)
  seg_end = seg.end

obj_paths = []
for seg in segments:
  for obj in seg.objs:
    obj_paths.append(obj.path)

set_paths = set(obj_paths)



# LoadTdsmFile('../../tests/Test_Log-20240731_122619.tdms')


    
    