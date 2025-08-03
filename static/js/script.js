$(document).ready(function() {
    // 初始化表格
    $('#srcTable').DataTable();
    
    // 表选择逻辑
    $('.table-check').change(function() {
        const table = $(this).data('table');
        const isChecked = $(this).prop('checked');
        
        // 自动选择所有列（实际实现需要处理列级选择）
        $(`tr:contains('${table}') .column-check`).prop('checked', isChecked);
    });
    
    // 生成脚本
    $('#generateBtn').click(function() {
        const selectedTables = [];
        $('.table-check:checked').each(function() {
            selectedTables.push($(this).data('table'));
        });
        
        const outputPath = $('#outputPath').val();
        
        $.ajax({
            url: '/generate',
            method: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({
                conn_id: "{{ conn_id }}",
                tables: selectedTables,
                output_path: outputPath
            }),
            success: function(response) {
                if(response.path) {
                    $('#scriptResult').html(`
                        <div class="alert alert-success">
                            Script saved to: ${response.path}
                        </div>
                    `);
                } else {
                    $('#scriptResult').html(`
                        <div class="alert alert-info">
                            <h4>Generated SQL:</h4>
                            <pre>${response.script}</pre>
                        </div>
                    `);
                }
            }
        });
    });
});