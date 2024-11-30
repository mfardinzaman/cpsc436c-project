import { Card, CardContent, Grid, Typography } from "@mui/material";

const RouteAllStats = ({ data }) => {
    return (
        <Card sx={{ marginBottom: 3 }}>
            <CardContent>
                <Grid container spacing={3}>
                    <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="h6" color="textSecondary">Average Delay</Typography>
                        <Typography variant="h4">{data.average_delay} min</Typography>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="h6" color="textSecondary">Median Delay</Typography>
                        <Typography variant="h4">{data.median_delay} min</Typography>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="h6" color="textSecondary">Vehicle Count</Typography>
                        <Typography variant="h4">{data.vehicle_count}</Typography>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="h6" color="textSecondary">Very Early Count</Typography>
                        <Typography variant="h4">{data.very_early_count}</Typography>
                    </Grid>
                    <Grid item xs={12} sm={6} md={3}>
                        <Typography variant="h6" color="textSecondary">Very Late Count</Typography>
                        <Typography variant="h4">{data.very_late_count}</Typography>
                    </Grid>
                </Grid>
            </CardContent>
        </Card>
    );
};