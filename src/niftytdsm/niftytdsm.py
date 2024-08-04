#from nptdms import TdmsFile
#tdms_file = TdmsFile.read("./Test_Log-20240731_122619.tdms")

with open('./Test_Log-20240731_122619.tdms', 'rb') as file:
    # Read the entire file into a bytes object
    file_content = file.read()


class TdsmMaskSettings:
  def __init__(self):
    self.meta_in_seg        = False
    self.raw_data_in_seg    = False
    self.Daqmx_data_in_seg  = False
    self.data_interleaved   = False
    self.data_is_big_endian = False
    self.new_obj_in_seg     = False

  def __str__(self):
    str_desc = ("Meta in Segment: " + str(self.meta_in_seg) + "\n" +
                "Raw Data in Segment: " + str(self.raw_data_in_seg) + "\n" +
                "Daqmx Data in Segment: " + str(self.Daqmx_data_in_seg) + "\n" +
                "Data Interleaved: " + str(self.data_interleaved) + "\n" +
                "Data is Big Endian: " + str(self.data_is_big_endian) + "\n" +
                "New Object in Segment: " + str(self.new_obj_in_seg))
    return str_desc


def TdsmExtractMaskSettings(mask):
  mask_meta_in_seg        = (1 << 1)
  mask_raw_data_in_seg    = (1 << 3)
  mask_Daqmx_data_in_seg  = (1 << 7)
  mask_data_interleaved   = (1 << 5)
  mask_data_is_big_endian = (1 << 6)
  mask_new_obj_in_seg     = (1 << 2)

  seg_mask_settings = TdsmMaskSettings()
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
  if (mask & mask_new_obj_in_seg):
    seg_mask_settings.new_obj_in_seg = True
  
  return seg_mask_settings


def ValidateTdsm(file_content):
  tdsm_status = True
  toc_mask, version, segment_len, meta_len = 0, 0, 0, 0

  valid_tdsm_tag  = b'\x54\x44\x53\x6D'
  tdsm_tag        = file_content[:4]
  if tdsm_tag != valid_tdsm_tag:
    print("Invalid TDSM file: Tag is not valid")
    tdsm_status = False

  else:
    toc_mask    = int.from_bytes(file_content[4:8], byteorder='little', signed=False)
    toc_mask_settings = TdsmExtractMaskSettings(toc_mask)
    
    if toc_mask_settings.data_is_big_endian == 1:
      bite_order = 'big'    
    else:
      bite_order = 'little'

    version     = int.from_bytes(file_content[8:12], byteorder=bite_order, signed=False)
    segment_len = int.from_bytes(file_content[12:20], byteorder=bite_order, signed=False)
    meta_len    = int.from_bytes(file_content[20:28], byteorder=bite_order, signed=False)

  return tdsm_status, toc_mask_settings, version, segment_len, meta_len


status, mask, ver, seg_len, meta_len = ValidateTdsm(file_content)
    
    