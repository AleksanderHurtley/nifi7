# NIFI is a bit different with bash commands. Using command path Bash and Argument Delimiter ;

-c;"
  # If the source audio directory does NOT exist, continue
  if [ ! -d ${source.audio.dir} ]; then
     echo 'No audio directory found at ${source.audio.dir}. Continuing.'
     exit 0;
  fi

  # If the source audio directory does exist and is empty, continue
  if [ -z "$(ls -A "${source.audio.dir}")" ]; then
     echo 'No files or catalogs found in audio directory at ${source.audio.dir}. Continuing.'
     exit 0;
  fi  
  
  # Ensure destination exists
  mkdir -p ${rep.data.dir}
  if [ $? != 0 ]; then
     echo 'Create directory ${rep.data.dir} failed'   >&2
     exit 1;
  fi

  # Attempt to copy everything (files and subfolders)
  cp -R ${source.audio.dir}/* ${rep.data.dir}/   >&2
  if [ $? != 0 ]; then
     echo '. Command cp -R ${source.audio.dir}/* ${rep.data.dir}/ failed.' >&2
     exit 1;
  fi

  # Normalize staged WAV filenames for downstream DPS submission.
  # One or more consecutive spaces are replaced with one hyphen.
  # Bash variables deliberately use $name without braces, because NiFi
  # interprets brace-style variables as NiFi Expression Language.
  rename_failed=0

  while IFS= read -r -d '' source_file; do
     directory=$(dirname "$source_file")
     filename=$(basename "$source_file")
     new_filename=$(printf '%s' "$filename" | sed -E 's/ +/-/g')
     target_file="$directory/$new_filename"

     if [ "$source_file" = "$target_file" ]; then
        continue;
     fi

     if [ -e "$target_file" ]; then
        echo "Cannot rename because target already exists: $target_file" >&2
        rename_failed=1
        continue;
     fi

     mv "$source_file" "$target_file"
     if [ $? != 0 ]; then
        echo "Failed to rename: $source_file" >&2
        rename_failed=1
     else
        echo "Renamed: $filename -> $new_filename"
     fi
  done < <(find ${rep.data.dir} -type f -iname '*.wav' -name '* *' -print0)

  if [ "$rename_failed" != 0 ]; then
     exit 1;
  fi

  echo 'Audio copy and filename normalization completed successfully or nothing to copy.'
"
