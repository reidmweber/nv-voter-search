$(document).ready(function() {
    console.log('Document ready');
    
    // Initialize DataTable with server-side processing
    let table = $('#voterTable').DataTable({
        processing: true,
        serverSide: true,
        ajax: {
            url: '/data',
            type: 'GET',
            dataType: 'json',
            dataSrc: function(json) {
                console.log('Received data:', json);
                return json.data || [];
            },
            error: function (xhr, error, thrown) {
                console.error('Error loading data:', error);
                console.error('Server response:', xhr.responseText);
                console.error('Status:', xhr.status);
                console.error('Error details:', thrown);
            }
        },
        columns: [
            { 
                data: 'STATE_VOTERID',
                defaultContent: ''
            },
            { 
                data: 'VOTER_NAME',
                defaultContent: ''
            },
            { 
                data: null,
                render: function(data, type, row) {
                    const parts = [
                        row.STREET_NUMBER,
                        row.STREET_PREDIRECTION,
                        row.STREET_NAME,
                        row.STREET_TYPE,
                        row.UNIT
                    ].filter(Boolean);
                    return parts.join(' ') || '';
                }
            },
            { 
                data: 'CITY',
                defaultContent: ''
            },
            { 
                data: 'STATE',
                defaultContent: ''
            },
            { 
                data: 'ZIP',
                defaultContent: ''
            },
            { 
                data: 'VOTER_REG_PARTY',
                defaultContent: ''
            },
            { 
                data: 'PRECINCT',
                defaultContent: ''
            },
            { 
                data: 'BALLOT_TYPE',
                defaultContent: ''
            },
            { 
                data: 'BALLOT_VOTE_METHOD',
                defaultContent: ''
            },
            { 
                data: 'VOTE_LOCATION',
                defaultContent: ''
            },
            { 
                data: 'BALLOT_STATUS',
                defaultContent: ''
            }
        ],
        pageLength: 25,
        order: [[0, 'asc']],
        dom: 'Bfrtip',
        buttons: ['copy', 'csv', 'excel'],
        language: {
            processing: "Loading data...",
            zeroRecords: "No matching records found",
            info: "Showing _START_ to _END_ of _TOTAL_ entries",
            infoEmpty: "No entries available",
            infoFiltered: "(filtered from _MAX_ total entries)"
        }
    });

    // Add error handling for stats
    $.get('/stats', function(data) {
        console.log('Stats data received:', data);
        
        // Original charts
        createChart('partyChart', 'Party Distribution', data.party_counts);
        createChart('cityChart', 'Top 10 Cities', data.city_counts);
        createChart('precinctChart', 'Top 10 Precincts', data.precinct_counts);
        
        // New charts with custom colors
        createChart('ballotStatusChart', 'Ballot Status', data.ballot_status_counts, {
            backgroundColor: [
                'rgba(75, 192, 192, 0.5)',  // Teal
                'rgba(255, 99, 132, 0.5)',   // Red
                'rgba(255, 206, 86, 0.5)',   // Yellow
                'rgba(153, 102, 255, 0.5)'   // Purple
            ]
        });
        
        createChart('voteMethodChart', 'Vote Method', data.vote_method_counts, {
            backgroundColor: [
                'rgba(54, 162, 235, 0.5)',   // Blue
                'rgba(255, 159, 64, 0.5)',   // Orange
                'rgba(75, 192, 192, 0.5)',   // Teal
                'rgba(255, 99, 132, 0.5)'    // Red
            ]
        });
        
        createChart('ballotTypeChart', 'Ballot Type', data.ballot_type_counts, {
            backgroundColor: [
                'rgba(153, 102, 255, 0.5)',  // Purple
                'rgba(255, 206, 86, 0.5)',   // Yellow
                'rgba(75, 192, 192, 0.5)',   // Teal
                'rgba(255, 99, 132, 0.5)'    // Red
            ]
        });
    }).fail(function(jqXHR, textStatus, errorThrown) {
        console.error('Error loading stats:', textStatus, errorThrown);
        console.error('Response:', jqXHR.responseText);
    });
});

function createChart(canvasId, label, data, options = {}) {
    const defaultOptions = {
        backgroundColor: 'rgba(54, 162, 235, 0.5)'
    };
    
    const chartOptions = { ...defaultOptions, ...options };
    
    new Chart(document.getElementById(canvasId), {
        type: 'bar',
        data: {
            labels: Object.keys(data),
            datasets: [{
                label: label,
                data: Object.values(data),
                backgroundColor: Array.isArray(chartOptions.backgroundColor) 
                    ? chartOptions.backgroundColor 
                    : chartOptions.backgroundColor
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: {
                    beginAtZero: true
                }
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) {
                                label += ': ';
                            }
                            label += context.parsed.y.toLocaleString();
                            return label;
                        }
                    }
                }
            }
        }
    });
} 