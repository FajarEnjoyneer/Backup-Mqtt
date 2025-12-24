local M = {}

local mosq   = require("mosquitto")
local socket = require("socket")
local lfs    = require("lfs")

local MQTT_CLOUD_HOST  = "103.150.226.102"
local MQTT_CLOUD_PORT  = 1883
local MQTT_CLOUD_TOPIC = "upload/data/backup"
local BACKUP_DIR       = "/usr/local/home/root/auto_backup/data"

local QOS            = 1
local KEEPALIVE      = 60

local function ensure_dir(path)
  if not lfs.attributes(path) then lfs.mkdir(path) end
end

function M.scan_and_upload()
  local client = mosq.new("mqtt_uploader_client_" .. tostring(os.time()))  
  ensure_dir(BACKUP_DIR)

  local files_to_upload = {}
  local current_time = os.time()

  for file in lfs.dir(BACKUP_DIR) do
    if file:match("%.json$") then
      local filepath = BACKUP_DIR .. "/" .. file
      local attr = lfs.attributes(filepath)
      if attr and attr.mode == "file" then
        if (current_time - attr.modification) > 60 then
          table.insert(files_to_upload, file)
        end
      end
    end
  end

  if #files_to_upload == 0 then 
    -- return if nothing to do
    return 
  end
  
  print(string.format("[UPLOADER] Found %d files ready to upload.", #files_to_upload))
  local connect_ok, res, err = pcall(function()
    return client:connect(MQTT_CLOUD_HOST, MQTT_CLOUD_PORT, KEEPALIVE)
  end)

  if not connect_ok or not res then
    print(string.format("[UPLOADER] Failed to connect to cloud broker: %s", tostring(err or res or "Unknown error")))
    return
  end
  client:loop_start()
  socket.sleep(2)

  print("[UPLOADER] Connected to cloud broker.")

  for _, filename in ipairs(files_to_upload) do
    local filepath = BACKUP_DIR .. "/" .. filename
    local file_handle = io.open(filepath, "r")

    if file_handle then
      print(string.format("[UPLOADER] Processing file: %s", filename))
      local line_count = 0
      local error_occurred = false
      local file_topic = MQTT_CLOUD_TOPIC

      for line in file_handle:lines() do
        if #line > 0 then
          client:publish(file_topic, line, QOS, false)
          socket.sleep(2)
          line_count = line_count + 1
        end
      end

      file_handle:close()

      if not error_occurred then
        print(string.format("[UPLOADER] Finished sending %d lines.", line_count))
        os.remove(filepath)
        print(string.format("[DELETE] File uploaded and deleted: %s", filename))
      else
        print("[UPLOADER] Upload failed mid-way. File not moved.")
        break
      end
    else
      print(string.format("[UPLOADER] Failed to open file: %s", filepath))
    end
  end

  client:disconnect()
  pcall(client.loop_stop, client, true)
  print("[UPLOADER] Upload session finished.")
end

return M
