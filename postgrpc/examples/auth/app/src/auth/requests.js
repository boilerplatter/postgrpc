const REACT_APP_KRATOS_PUBLIC_URL = process.env.REACT_APP_KRATOS_PUBLIC_URL

// request options for all Kratos requests
const REQUEST_OPTIONS = {
  credentials: 'include',
  headers: {
    Accept: 'application/json'
  }
}

// log out of the current session
export const logOut = async () => {
  const response = await fetch(
    `${REACT_APP_KRATOS_PUBLIC_URL}/self-service/logout/browser`,
    REQUEST_OPTIONS
  )

  const { logout_url: url } = await response.json()

  return fetch(url, REQUEST_OPTIONS)
}

// start a single login flow
export const startLogin = async () => {
  const response = await fetch(
    `${REACT_APP_KRATOS_PUBLIC_URL}/self-service/login/browser`,
    REQUEST_OPTIONS,
  )

  return response.json()
}

// complete an existing login flow
export const completeLogin = async (flow, form) => {
  const entries = form.entries()
  const body = new URLSearchParams()
  body.append('method', 'password')

  for (const [key, value] of entries) {
    body.append(key, value)
  }

  const response = await fetch(
    flow.action,
    {
      body,
      method: flow.method,
      ...REQUEST_OPTIONS,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        ...REQUEST_OPTIONS.headers
      }
    }
  )

  return response.json()
}

// start a single registration flow
export const startRegistration = async () => {
  const response = await fetch(
    `${REACT_APP_KRATOS_PUBLIC_URL}/self-service/registration/browser`,
    REQUEST_OPTIONS,
  )

  return response.json()
}

// complete an existing registration flow
export const completeRegistration = async (flow, form) => {
  const entries = form.entries()
  const body = new URLSearchParams()
  body.append('method', 'password')

  for (const [key, value] of entries) {
    body.append(key, value)
  }

  const response = await fetch(
    flow.action,
    {
      body,
      method: flow.method,
      ...REQUEST_OPTIONS,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        ...REQUEST_OPTIONS.headers
      }
    }
  )

  return response.json()
}

// get the active session (if one exists)
export const getSession = async () => {
  const response = await fetch(
    `${REACT_APP_KRATOS_PUBLIC_URL}/sessions/whoami`,
    REQUEST_OPTIONS,
  )

  return response.json()
}

