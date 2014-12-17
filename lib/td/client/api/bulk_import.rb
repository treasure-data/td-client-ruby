class TreasureData::API
module BulkImport

  ####
  ## Bulk import API
  ##

  # => nil
  def create_bulk_import(name, db, table, opts={})
    params = opts.dup
    code, body, res = post("/v3/bulk_import/create/#{e name}/#{e db}/#{e table}", params)
    if code != "200"
      raise_error("Create bulk import failed", res)
    end
    return nil
  end

  # => nil
  def delete_bulk_import(name, opts={})
    params = opts.dup
    code, body, res = post("/v3/bulk_import/delete/#{e name}", params)
    if code != "200"
      raise_error("Delete bulk import failed", res)
    end
    return nil
  end

  # => data:Hash
  def show_bulk_import(name)
    code, body, res = get("/v3/bulk_import/show/#{name}")
    if code != "200"
      raise_error("Show bulk import failed", res)
    end
    js = checked_json(body, %w[status])
    return js
  end

  # => result:[data:Hash]
  def list_bulk_imports(opts={})
    params = opts.dup
    code, body, res = get("/v3/bulk_import/list", params)
    if code != "200"
      raise_error("List bulk imports failed", res)
    end
    js = checked_json(body, %w[bulk_imports])
    return js['bulk_imports']
  end

  def list_bulk_import_parts(name, opts={})
    params = opts.dup
    code, body, res = get("/v3/bulk_import/list_parts/#{e name}", params)
    if code != "200"
      raise_error("List bulk import parts failed", res)
    end
    js = checked_json(body, %w[parts])
    return js['parts']
  end

  # => nil
  def bulk_import_upload_part(name, part_name, stream, size, opts={})
    code, body, res = put("/v3/bulk_import/upload_part/#{e name}/#{e part_name}", stream, size)
    if code[0] != ?2
      raise_error("Upload a part failed", res)
    end
    return nil
  end

  # => nil
  def bulk_import_delete_part(name, part_name, opts={})
    params = opts.dup
    code, body, res = post("/v3/bulk_import/delete_part/#{e name}/#{e part_name}", params)
    if code[0] != ?2
      raise_error("Delete a part failed", res)
    end
    return nil
  end

  # => nil
  def freeze_bulk_import(name, opts={})
    params = opts.dup
    code, body, res = post("/v3/bulk_import/freeze/#{e name}", params)
    if code != "200"
      raise_error("Freeze bulk import failed", res)
    end
    return nil
  end

  # => nil
  def unfreeze_bulk_import(name, opts={})
    params = opts.dup
    code, body, res = post("/v3/bulk_import/unfreeze/#{e name}", params)
    if code != "200"
      raise_error("Unfreeze bulk import failed", res)
    end
    return nil
  end

  # => jobId:String
  def perform_bulk_import(name, opts={})
    params = opts.dup
    code, body, res = post("/v3/bulk_import/perform/#{e name}", params)
    if code != "200"
      raise_error("Perform bulk import failed", res)
    end
    js = checked_json(body, %w[job_id])
    return js['job_id'].to_s
  end

  # => nil
  def commit_bulk_import(name, opts={})
    params = opts.dup
    code, body, res = post("/v3/bulk_import/commit/#{e name}", params)
    if code != "200"
      raise_error("Commit bulk import failed", res)
    end
    return nil
  end

  # => data...
  def bulk_import_error_records(name, opts={}, &block)
    params = opts.dup
    code, body, res = get("/v3/bulk_import/error_records/#{e name}", params)
    if code != "200"
      raise_error("Failed to get bulk import error records", res)
    end
    if body.nil? || body.empty?
      if block
        return nil
      else
        return []
      end
    end
    require File.expand_path('../compat_gzip_reader', File.dirname(__FILE__))
    u = MessagePack::Unpacker.new(Zlib::GzipReader.new(StringIO.new(body)))
    if block
      begin
        u.each(&block)
      rescue EOFError
      end
      nil
    else
      result = []
      begin
        u.each {|row|
          result << row
        }
      rescue EOFError
      end
      return result
    end
  end

end
end
