import { ClipLoader } from 'react-spinners'
import { useIdentity } from './auth'

const Dashboard = () => {
  const [identity, logOut] = useIdentity()

  const onLogOut = async event => {
    event.preventDefault()
    await logOut()
  }

  return identity
    ? <>
      <button onClick={onLogOut}>Log out</button>
      <h1>{identity.traits.name.first}'s Notes:</h1>
      <p>There should be some notes here soon!</p>
    </>
    : <ClipLoader color='white' />
}

export default Dashboard
