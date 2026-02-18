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

  echo 'Audio copy completed successfully or nothing to copy.'
"