magic
id
batch_len
rec_number
  flag
  flag
svbyte 编码(长度 = batch_len - rec_number)
  key_len
  key_offset / key_file_id （对大于 1mb 的 key 或者 val 会单独存为文件，通过 flag 来区分）
  val_len
  val_offset / val_file_id （对大于 1mb 的 val 或者 val 会单独存为文件, 通过 flag 来区分）
batch_crc32