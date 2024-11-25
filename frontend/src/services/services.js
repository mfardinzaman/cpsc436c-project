import fetchHelper from "./fetchHelper";

const getRoutes = async () => {
    const data = await fetchHelper('/routes', 'GET', {})
    return data
}

const service = { getRoutes }
export default service