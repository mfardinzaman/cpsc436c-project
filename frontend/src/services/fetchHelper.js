const ENDPOINT = 'https://au2zs2rnck.execute-api.ca-central-1.amazonaws.com/prod'

const fetchHelper = async (path, method, body) => {

  const response = await fetch(ENDPOINT + path, {
    method,
    headers: {
      'Content-Type': 'application/json',
    },
    [method.toUpperCase() !== 'GET' ? 'body' : undefined]: JSON.stringify(body),
  })
  const data = await response.json()

  return data
}

export default fetchHelper