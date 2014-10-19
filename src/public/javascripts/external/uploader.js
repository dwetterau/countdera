// array of files we are going to upload

(function() {
    // add for regular upload
    var uploadmap = document.getElementById('map_upload');
    uploadmap.addEventListener('change', handleFileSelect, false);

    // add drop and drag
    var mapdrop = document.getElementById('map_drop');
    mapdrop.addEventListener('dragover', handleDragOver, false);
    mapdrop.addEventListener('drop', handleDropSelect, false);

    var uploadreduce = document.getElementById('reduce_upload');
    uploadreduce.addEventListener('change', handleFileSelect, false);

    // add drop and drag
    var reducedrop = document.getElementById('reduce_drop');
    reducedrop.addEventListener('dragover', handleDragOver, false);
    reducedrop.addEventListener('drop', handleDropSelect, false);



    function handleFileSelect(evt) {
        evt.stopPropagation();
        evt.preventDefault();

        var fileList = evt.target.files;
        handleFiles(fileList);
    }

    function handleDropSelect(evt) {
        evt.stopPropagation();
        evt.preventDefault();

        var fileList = evt.dataTransfer.files;
        handleFiles(fileList);
    }

    function handleDragOver(evt) {
        evt.stopPropagation();
        evt.preventDefault();
        evt.dataTransfer.dropEffect = 'copy'; // Explicitly show this is a copy.
    }

    function handleFiles(fileList) {
        var f =fileList[0]
        var reader = new FileReader();

        reader.onload = (function(theFile) {
            return function(e) {
                if($('#map_code_area').is(":visible"))
                    $('#map_code_area').html(e.target.result);
                else
                    $('#reduce_code_area').html(e.target.result);

            };
        })(f);
        reader.readAsText(f);
    }
})();